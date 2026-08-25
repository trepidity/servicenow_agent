use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rpc) enum BusinessApplicationLookup {
    SysId(String),
    Name(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::rpc) struct BusinessApplicationHydrationOptions {
    #[serde(default = "default_true")]
    pub(in crate::rpc) persist: bool,
    #[serde(default = "default_true")]
    pub(in crate::rpc) resolve_references: bool,
    #[serde(default = "default_reference_depth")]
    pub(in crate::rpc) reference_depth: usize,
    #[serde(default)]
    pub(in crate::rpc) refresh_dictionary: bool,
}

impl Default for BusinessApplicationHydrationOptions {
    fn default() -> Self {
        Self {
            persist: true,
            resolve_references: true,
            reference_depth: 1,
            refresh_dictionary: false,
        }
    }
}

impl From<BusinessApplicationHydrationOptions> for snow_core::BusinessApplicationHydrationOptions {
    fn from(options: BusinessApplicationHydrationOptions) -> Self {
        Self {
            persist: options.persist,
            resolve_references: options.resolve_references,
            reference_depth: options.reference_depth,
            refresh_dictionary: options.refresh_dictionary,
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::rpc) struct BusinessApplicationSyncRequest {
    pub(in crate::rpc) all: bool,
    pub(in crate::rpc) search_params: Option<snow_core::BusinessApplicationSearchParams>,
    pub(in crate::rpc) options: BusinessApplicationHydrationOptions,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(in crate::rpc) struct BusinessApplicationQueryParams {
    #[serde(default)]
    pub(in crate::rpc) text: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) filters: Vec<BusinessApplicationFieldFilter>,
    #[serde(default)]
    pub(in crate::rpc) include_tombstoned: bool,
    #[serde(default)]
    pub(in crate::rpc) limit: Option<usize>,
    #[serde(default)]
    pub(in crate::rpc) offset: Option<usize>,
    #[serde(default)]
    pub(in crate::rpc) sort: Vec<BusinessApplicationSortField>,
}

#[derive(Debug, Clone, Deserialize)]
pub(in crate::rpc) struct BusinessApplicationFieldFilter {
    pub(in crate::rpc) field: String,
    pub(in crate::rpc) op: String,
    #[serde(default)]
    pub(in crate::rpc) value: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(in crate::rpc) struct BusinessApplicationSortField {
    pub(in crate::rpc) field: String,
    #[serde(default)]
    pub(in crate::rpc) direction: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(in crate::rpc) struct BusinessApplicationFieldsParams {
    #[serde(default)]
    pub(in crate::rpc) refresh_dictionary: bool,
}

/// One entry of the `business_application_fields` response.
///
/// `observed_count`/`sample_*` come from the locally projected Business
/// Application records. When dictionary metadata is present the `label`,
/// `field_type`, `reference_table`, `mandatory`, `read_only`, `choice`, and
/// `max_length` fields are merged in and `dictionary_verified` is `true`;
/// otherwise those remain `None`/`false` and the entry falls back to
/// observed-only behavior.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(in crate::rpc) struct BusinessApplicationFieldSummary {
    pub(in crate::rpc) field: String,
    pub(in crate::rpc) observed_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) sample_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) sample_display_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) field_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) reference_table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) mandatory: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) choice: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) max_length: Option<i64>,
    pub(in crate::rpc) dictionary_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) diagnostic: Option<String>,
}

pub(in crate::rpc) fn default_true() -> bool {
    true
}

pub(in crate::rpc) fn default_reference_depth() -> usize {
    1
}

pub(in crate::rpc) fn extract_business_application_search_params(
    params: &Value,
) -> Result<(
    snow_core::BusinessApplicationSearchParams,
    BusinessApplicationHydrationOptions,
)> {
    let mut search_params = params.clone();
    let mut options = BusinessApplicationHydrationOptions::default();
    if let Value::Object(map) = &mut search_params {
        if let Some(value) = map.remove("persist").and_then(|value| value.as_bool()) {
            options.persist = value;
        }
        if let Some(value) = map
            .remove("resolve_references")
            .and_then(|value| value.as_bool())
        {
            options.resolve_references = value;
        }
        if let Some(value) = map
            .remove("reference_depth")
            .and_then(|value| value.as_u64())
        {
            if value > 2 {
                return Err(anyhow!("`reference_depth` must be 0, 1, or 2"));
            }
            options.reference_depth = value as usize;
        }
        if let Some(value) = map
            .remove("refresh_dictionary")
            .and_then(|value| value.as_bool())
        {
            options.refresh_dictionary = value;
        }
    }
    let params: snow_core::BusinessApplicationSearchParams = serde_json::from_value(search_params)?;
    params.validate()?;
    Ok((params, options))
}

/// Parse `business_application_sync` params: optional search params plus
/// hydration options. Unlike search, the search params are optional; when no
/// search field is supplied we pass `None` so core runs the default bounded
/// Business Application search.
pub(in crate::rpc) fn extract_business_application_sync_params(
    params: &Value,
) -> Result<BusinessApplicationSyncRequest> {
    let mut sync_params = params.clone();
    let mut all = false;
    if let Value::Object(map) = &mut sync_params {
        match map.remove("all") {
            Some(Value::Bool(value)) => all = value,
            Some(_) => return Err(anyhow!("`all` must be a boolean")),
            None => {}
        }
    }
    let (search_params, options) = extract_business_application_search_params(&sync_params)?;
    // Treat an all-default params object (no filters set) as "no search params".
    let has_filter = search_params != snow_core::BusinessApplicationSearchParams::default();
    if all && has_filter {
        return Err(anyhow!(
            "`all` cannot be combined with Business Application search filters"
        ));
    }
    Ok(BusinessApplicationSyncRequest {
        all,
        search_params: (!all && has_filter).then_some(search_params),
        options,
    })
}

pub(in crate::rpc) fn extract_business_application_hydration_options(
    params: &Value,
) -> Result<BusinessApplicationHydrationOptions> {
    let mut options = BusinessApplicationHydrationOptions::default();
    let Value::Object(map) = params else {
        return Ok(options);
    };
    if let Some(value) = map.get("persist").and_then(Value::as_bool) {
        options.persist = value;
    }
    if let Some(value) = map.get("resolve_references").and_then(Value::as_bool) {
        options.resolve_references = value;
    }
    if let Some(value) = map.get("reference_depth").and_then(Value::as_u64) {
        if value > 2 {
            return Err(anyhow!("`reference_depth` must be 0, 1, or 2"));
        }
        options.reference_depth = value as usize;
    }
    if let Some(value) = map.get("refresh_dictionary").and_then(Value::as_bool) {
        options.refresh_dictionary = value;
    }
    Ok(options)
}

pub(in crate::rpc) fn extract_business_application_lookup_params(
    params: &Value,
) -> Result<BusinessApplicationLookup> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let sys_id = map.get("sys_id").and_then(Value::as_str).map(str::trim);
    let name = map.get("name").and_then(Value::as_str).map(str::trim);
    match (
        sys_id.filter(|value| !value.is_empty()),
        name.filter(|value| !value.is_empty()),
    ) {
        (Some(sys_id), None) => Ok(BusinessApplicationLookup::SysId(
            snow_core::normalize_record_lookup_sys_id(sys_id)?,
        )),
        (None, Some(name)) => Ok(BusinessApplicationLookup::Name(name.to_string())),
        (Some(_), Some(_)) => Err(anyhow!("provide exactly one of `sys_id` or `name`")),
        (None, None) => Err(anyhow!(
            "missing required lookup: provide `sys_id` or `name`"
        )),
    }
}

pub(in crate::rpc) fn extract_business_application_query_params(
    params: &Value,
) -> Result<BusinessApplicationQueryParams> {
    let query: BusinessApplicationQueryParams = serde_json::from_value(params.clone())?;
    if query.limit == Some(0) {
        return Err(anyhow!("`limit` must be at least 1"));
    }
    if query.limit.unwrap_or(20) > 500 {
        return Err(anyhow!("`limit` must be at most 500"));
    }
    Ok(query)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rpc) struct BusinessApplicationServersRpcParams {
    pub(in crate::rpc) traversal: snow_core::BusinessApplicationServersParams,
    pub(in crate::rpc) persist: bool,
    pub(in crate::rpc) prune_stale: bool,
}

/// Deserialize an incoming `business_application_servers` request into the
/// canonical [`snow_core::BusinessApplicationServersParams`] traversal contract
/// plus daemon-level persistence controls, then run validation up-front.
///
/// The core type owns `#[serde(deny_unknown_fields)]` and
/// [`snow_core::BusinessApplicationServersParams::validate`], so unknown fields,
/// the selector XOR (`number` vs `sys_id`), the `BA:<sys_id>` fallback guard,
/// the traversal bounds (`max_depth`/`max_cis`/`max_edges`) and selector
/// normalization are all enforced in one place. Validating here (instead of
/// leaning on the re-validation inside `SnowCore::business_application_servers`)
/// is what lets the dispatcher classify a validation failure as
/// `invalid_params` rather than a service/internal error.
pub(in crate::rpc) fn parse_business_application_servers_params(
    params: &Value,
) -> Result<BusinessApplicationServersRpcParams> {
    let mut traversal_params = params.clone();
    let mut persist = true;
    let mut prune_stale = false;
    if let Value::Object(map) = &mut traversal_params {
        match map.remove("persist") {
            Some(Value::Bool(value)) => persist = value,
            Some(_) => return Err(anyhow!("`persist` must be a boolean")),
            None => {}
        }
        match map.remove("prune_stale") {
            Some(Value::Bool(value)) => prune_stale = value,
            Some(_) => return Err(anyhow!("`prune_stale` must be a boolean")),
            None => {}
        }
    }
    if prune_stale && !persist {
        return Err(anyhow!("`prune_stale` requires `persist=true`"));
    }
    let params: snow_core::BusinessApplicationServersParams =
        serde_json::from_value(traversal_params)?;
    // Surface validation errors here so the caller maps them to invalid_params.
    // The resulting options are discarded; core re-validates during traversal.
    params.validate()?;
    Ok(BusinessApplicationServersRpcParams {
        traversal: params,
        persist,
        prune_stale,
    })
}

pub(in crate::rpc) fn parse_business_application_servers_cached_params(
    params: &Value,
) -> Result<snow_core::BusinessApplicationServersCachedParams> {
    let params: snow_core::BusinessApplicationServersCachedParams =
        serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

pub(in crate::rpc) fn parse_business_applications_for_server_params(
    params: &Value,
) -> Result<snow_core::BusinessApplicationsForServerParams> {
    let params: snow_core::BusinessApplicationsForServerParams =
        serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

pub(in crate::rpc) fn extract_business_application_fields_params(
    params: &Value,
) -> Result<BusinessApplicationFieldsParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(in crate::rpc) async fn get_business_application_cached(
    core: &SnowCore,
    lookup: &BusinessApplicationLookup,
) -> Result<Option<SnowRecord>> {
    let records = core
        .list_records_query(
            ListQuery::new()
                .resource_type(ResourceType::BusinessApplication)
                .include_tombstoned(false),
        )
        .await?;
    Ok(records.into_iter().find(|record| match lookup {
        BusinessApplicationLookup::SysId(sys_id) => record.sys_id.eq_ignore_ascii_case(sys_id),
        BusinessApplicationLookup::Name(name) => business_application_name(record) == name.trim(),
    }))
}

pub(in crate::rpc) fn core_business_application_lookup(
    lookup: BusinessApplicationLookup,
) -> Result<snow_core::BusinessApplicationLookup> {
    Ok(match lookup {
        BusinessApplicationLookup::SysId(sys_id) => {
            snow_core::BusinessApplicationLookup::sys_id(sys_id)?
        }
        BusinessApplicationLookup::Name(name) => {
            snow_core::BusinessApplicationLookup::exact_name(name)
        }
    })
}

pub(in crate::rpc) fn business_application_get_result(
    transport: &DaemonTransport<'_>,
    record: &SnowRecord,
    source: snow_core::Source,
    completeness: snow_core::Completeness,
    live_without_local_io: bool,
) -> Result<Value> {
    let business_application = if live_without_local_io {
        transport.live_business_application(record)?
    } else {
        transport.business_application(record)?
    };
    let record_dto = business_application.record.clone();
    operation_envelope_result(
        "business_application_get",
        source,
        completeness,
        json!({
        "business_application": business_application,
        "record": record_dto,
        "markdown": render_snow_record(record),
        }),
    )
}

pub(in crate::rpc) fn operation_envelope_result(
    operation: &str,
    source: snow_core::Source,
    completeness: snow_core::Completeness,
    data: Value,
) -> Result<Value> {
    Ok(serde_json::to_value(snow_core::OperationEnvelope {
        operation: operation.to_string(),
        source,
        completeness,
        data,
    })?)
}

pub(in crate::rpc) async fn query_business_applications_local(
    core: &SnowCore,
    params: &BusinessApplicationQueryParams,
) -> Result<Vec<SnowRecord>> {
    core.query_business_applications(core_business_application_query(params)?)
        .await
}

pub(in crate::rpc) async fn business_application_cache_snapshot(
    core: &SnowCore,
) -> Result<Option<(Vec<SnowRecord>, chrono::DateTime<chrono::Utc>)>> {
    let records = core
        .list_records_query(
            ListQuery::new()
                .resource_type(ResourceType::BusinessApplication)
                .include_tombstoned(false),
        )
        .await?;
    let Some(last_refreshed_at) = records.iter().map(|record| record.synced_at).min() else {
        return Ok(None);
    };
    Ok(Some((records, last_refreshed_at)))
}

pub(in crate::rpc) fn cached_business_application_search_query(
    params: &snow_core::BusinessApplicationSearchParams,
) -> snow_core::query::filter::BusinessApplicationQuery {
    use snow_core::query::filter::{BusinessApplicationQuery, FieldOperator};

    let mut query = BusinessApplicationQuery::new()
        .limit(params.limit.unwrap_or(20))
        .allow_unknown_fields(true);
    for (field, value, op) in [
        ("name", params.name.as_ref(), FieldOperator::Contains),
        (
            "business_owner",
            params.business_owner.as_ref(),
            FieldOperator::Contains,
        ),
        (
            "it_application_owner",
            params.is_owner.as_ref(),
            FieldOperator::Contains,
        ),
        (
            "managed_by_group",
            params.ci_owner_group.as_ref(),
            FieldOperator::Contains,
        ),
        (
            "support_group",
            params.primary_support_group.as_ref(),
            FieldOperator::Contains,
        ),
        (
            "operational_status",
            params.operational_state.as_ref(),
            FieldOperator::Eq,
        ),
        (
            "operational_status",
            params.operational_state_not.as_ref(),
            FieldOperator::Ne,
        ),
        (
            "portfolio",
            params.primary_portfolio.as_ref(),
            FieldOperator::Contains,
        ),
        (
            "attested_date",
            params.attested_date.as_ref(),
            FieldOperator::Eq,
        ),
        (
            "attested_date",
            params.attested_date_on_or_after.as_ref(),
            FieldOperator::Gte,
        ),
        (
            "attested_date",
            params.attested_date_on_or_before.as_ref(),
            FieldOperator::Lte,
        ),
    ] {
        if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
            query = query.filter(field, op, json!(value));
        }
    }
    query
}

pub(in crate::rpc) fn business_application_cache_miss(
    id: Option<Value>,
    operation: &str,
) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32072,
        "cache miss",
        Some(json!({
            "code": "CACHE_MISS",
            "operation": operation,
            "object": "business_application"
        })),
    )
}

pub(in crate::rpc) fn core_business_application_query(
    params: &BusinessApplicationQueryParams,
) -> Result<snow_core::query::filter::BusinessApplicationQuery> {
    let filters = params
        .filters
        .iter()
        .map(|filter| {
            Ok(snow_core::query::filter::FieldFilter {
                field: filter.field.clone(),
                op: core_field_operator(filter.op.as_str())?,
                value: filter.value.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let sort = params
        .sort
        .iter()
        .map(|field| snow_core::query::filter::SortField {
            field: field.field.clone(),
            direction: if field
                .direction
                .as_deref()
                .is_some_and(|direction| direction.eq_ignore_ascii_case("desc"))
            {
                snow_core::query::filter::SortDirection::Desc
            } else {
                snow_core::query::filter::SortDirection::Asc
            },
        })
        .collect();

    Ok(snow_core::query::filter::BusinessApplicationQuery {
        text: params.text.clone(),
        filters,
        include_tombstoned: params.include_tombstoned,
        limit: params.limit,
        offset: params.offset,
        sort,
        allow_unknown_fields: true,
    })
}

pub(in crate::rpc) fn core_field_operator(
    op: &str,
) -> Result<snow_core::query::filter::FieldOperator> {
    Ok(match op.trim().to_ascii_lowercase().as_str() {
        "eq" => snow_core::query::filter::FieldOperator::Eq,
        "ne" => snow_core::query::filter::FieldOperator::Ne,
        "contains" => snow_core::query::filter::FieldOperator::Contains,
        "starts_with" | "startswith" => snow_core::query::filter::FieldOperator::StartsWith,
        "in" => snow_core::query::filter::FieldOperator::In,
        "is_empty" => snow_core::query::filter::FieldOperator::IsEmpty,
        "is_not_empty" => snow_core::query::filter::FieldOperator::IsNotEmpty,
        "gt" => snow_core::query::filter::FieldOperator::Gt,
        "gte" => snow_core::query::filter::FieldOperator::Gte,
        "lt" => snow_core::query::filter::FieldOperator::Lt,
        "lte" => snow_core::query::filter::FieldOperator::Lte,
        other => {
            return Err(anyhow!(
                "unsupported Business Application field operator `{other}`"
            ));
        }
    })
}

pub(in crate::rpc) async fn business_application_servers(
    core: &SnowCore,
    transport: &DaemonTransport<'_>,
    params: snow_core::BusinessApplicationServersParams,
) -> Result<Option<Value>> {
    let Some(result) = core.business_application_servers(params).await? else {
        return Ok(None);
    };

    let result_value = serde_json::to_value(&result)?;
    let mut servers = Vec::with_capacity(result.servers.len());
    for server in result.servers {
        // Per-server `source` tag. Traversal servers are the default
        // (`cmdb_rel_ci`) and are omitted from `server_sources`, so only
        // `ci_owner_group` fallback servers carry an explicit source here.
        let source = result.server_sources.get(&server.record.sys_id).copied();
        let mut server_value = serde_json::to_value(transport.server(&server.record)?)?;
        if let (Some(source), Some(object)) = (source, server_value.as_object_mut()) {
            object.insert("source".to_string(), json!(source.as_str()));
        }
        servers.push(server_value);
    }

    let mut response = json!({
        "business_application": result.business_application,
        "servers": servers,
        "relationship_summary": result.relationship_summary,
        "diagnostics": result.diagnostics,
        "server_paths": result.server_paths,
    });
    if let (Some(response), Some(result_value)) =
        (response.as_object_mut(), result_value.as_object())
    {
        for (key, value) in result_value {
            // The per-server `source` tag is already attached to each server
            // above; the top-level `server_sources` map is an internal merge
            // helper, not part of the response contract, so it is not surfaced.
            if key == "server_sources" {
                continue;
            }
            response.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    Ok(Some(response))
}

pub(in crate::rpc) async fn business_application_servers_cached(
    core: &SnowCore,
    transport: &DaemonTransport<'_>,
    params: snow_core::BusinessApplicationServersCachedParams,
) -> Result<Option<Value>> {
    let Some(result) = core.business_application_servers_cached(params).await? else {
        return Ok(None);
    };

    let business_application = transport.business_application(&result.business_application)?;
    let mut servers = Vec::with_capacity(result.servers.len());
    for relationship in result.servers {
        servers.push(json!({
            "server": transport.server(&relationship.server)?,
            "server_table": relationship.server_table,
            "provenance": relationship.provenance,
            "min_depth": relationship.min_depth,
            "paths": relationship.paths,
            "tombstoned_at": relationship.tombstoned_at,
        }));
    }

    Ok(Some(json!({
        "business_application": business_application,
        "servers": servers,
        "endpoint_status": result.endpoint_status,
        "relationship_status": result.relationship_status,
        "inventory_health": result.inventory_health,
    })))
}

pub(in crate::rpc) async fn business_applications_for_server(
    core: &SnowCore,
    transport: &DaemonTransport<'_>,
    params: snow_core::BusinessApplicationsForServerParams,
) -> Result<Option<Value>> {
    let Some(result) = core.business_applications_for_server(params).await? else {
        return Ok(None);
    };

    let mut servers = Vec::with_capacity(result.servers.len());
    for server_relationships in result.servers {
        let mut business_applications =
            Vec::with_capacity(server_relationships.business_applications.len());
        for relationship in server_relationships.business_applications {
            business_applications.push(json!({
                "business_application": transport.business_application(&relationship.business_application)?,
                "provenance": relationship.provenance,
                "min_depth": relationship.min_depth,
                "paths": relationship.paths,
                "inventory_health": relationship.inventory_health,
                "tombstoned_at": relationship.tombstoned_at,
            }));
        }
        servers.push(json!({
            "server": transport.server(&server_relationships.server)?,
            "business_applications": business_applications,
            "endpoint_status": server_relationships.endpoint_status,
            "relationship_status": server_relationships.relationship_status,
        }));
    }

    Ok(Some(json!({
        "servers": servers,
        "endpoint_status": result.endpoint_status,
        "relationship_status": result.relationship_status,
    })))
}

pub(in crate::rpc) async fn business_application_fields(
    core: &SnowCore,
    params: BusinessApplicationFieldsParams,
) -> Result<Vec<BusinessApplicationFieldSummary>> {
    // When requested, refresh the live dictionary before reading. Best-effort:
    // a refresh failure leaves us with whatever cached/observed data exists.
    if params.refresh_dictionary {
        let _ = core.refresh_business_application_dictionary().await;
    }

    // Load cached dictionary metadata (empty map => degraded/observed-only mode).
    let dictionary = core
        .business_application_dictionary()
        .await
        .unwrap_or_default();

    let records = core
        .list_records_query(ListQuery::new().resource_type(ResourceType::BusinessApplication))
        .await?;
    let mut fields: std::collections::BTreeMap<String, BusinessApplicationFieldSummary> =
        std::collections::BTreeMap::new();

    // Seed entries from the dictionary so verified fields appear even when no
    // record has yet been observed locally.
    for (name, row) in &dictionary {
        fields
            .entry(name.clone())
            .or_insert_with(|| dictionary_field_summary(name, row));
    }

    for record in records {
        for (name, value) in record.fields {
            let entry = fields.entry(name.clone()).or_insert_with(|| {
                // No dictionary row for this observed field: fall back to the
                // observed-only summary, attaching a degraded diagnostic when a
                // dictionary was expected but is unavailable.
                BusinessApplicationFieldSummary {
                    field: name.clone(),
                    observed_count: 0,
                    sample_value: None,
                    sample_display_value: None,
                    label: None,
                    field_type: None,
                    reference_table: None,
                    mandatory: None,
                    read_only: None,
                    choice: None,
                    max_length: None,
                    dictionary_verified: false,
                    diagnostic: (params.refresh_dictionary && dictionary.is_empty()).then(|| {
                        "dictionary unavailable; field metadata is observed-only".to_string()
                    }),
                }
            });
            entry.observed_count += 1;
            if entry.sample_value.is_none() && !value.value.trim().is_empty() {
                entry.sample_value = Some(value.value);
            }
            if entry.sample_display_value.is_none() {
                entry.sample_display_value = value
                    .display_value
                    .filter(|display| !display.trim().is_empty());
            }
        }
    }
    Ok(fields.into_values().collect())
}

/// Build an enriched field summary from a cached dictionary row.
pub(in crate::rpc) fn dictionary_field_summary(
    name: &str,
    row: &snow_core::cache::store::BusinessApplicationFieldDictionaryRow,
) -> BusinessApplicationFieldSummary {
    BusinessApplicationFieldSummary {
        field: name.to_string(),
        observed_count: 0,
        sample_value: None,
        sample_display_value: None,
        label: row.field_label.clone(),
        field_type: row.field_type.clone(),
        reference_table: row.reference_table.clone(),
        mandatory: Some(row.mandatory),
        read_only: Some(row.read_only),
        choice: Some(row.choice),
        max_length: row.max_length,
        dictionary_verified: true,
        diagnostic: None,
    }
}

pub(in crate::rpc) fn business_application_diagnostics(
    diagnostics: &[snow_core::ReferenceResolutionDiagnostic],
) -> Vec<DaemonBusinessApplicationDiagnostic> {
    diagnostics
        .iter()
        .map(|diagnostic| DaemonBusinessApplicationDiagnostic {
            field: diagnostic.field.clone(),
            sys_id: (!diagnostic.reference_sys_id.is_empty())
                .then(|| diagnostic.reference_sys_id.clone()),
            table: (!diagnostic.reference_table.is_empty())
                .then(|| diagnostic.reference_table.clone()),
            diagnostic: diagnostic.message.clone().unwrap_or_else(|| {
                format!("{:?}", diagnostic.reason)
                    .chars()
                    .enumerate()
                    .flat_map(|(idx, ch)| {
                        if idx > 0 && ch.is_ascii_uppercase() {
                            vec!['_', ch.to_ascii_lowercase()]
                        } else {
                            vec![ch.to_ascii_lowercase()]
                        }
                    })
                    .collect()
            }),
        })
        .collect()
}

pub(in crate::rpc) fn business_application_name(record: &SnowRecord) -> String {
    record
        .fields
        .get("name")
        .and_then(|field| {
            field
                .display_value
                .as_ref()
                .or(Some(&field.value))
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

pub(in crate::rpc) async fn dispatch_business_applications(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::BusinessApplicationGet => {
            match extract_business_application_lookup_params(&request.params) {
                Ok(lookup) => {
                    let rule = state
                        .cache_policy
                        .active()
                        .rule_for("business_application_get", "business_application");
                    let cached = if rule.mode == snow_core::cache::policy::CacheMode::Live {
                        Ok(None)
                    } else {
                        get_business_application_cached(state.core.as_ref(), &lookup)
                            .await
                            .map(|record| {
                                record.filter(|record| {
                                    rule.mode == snow_core::cache::policy::CacheMode::CacheOnly
                                        || rule.ttl.is_some_and(|ttl| {
                                            record.synced_at + ttl > chrono::Utc::now()
                                        })
                                })
                            })
                    };
                    match cached {
                        Ok(Some(record)) => match business_application_get_result(
                            transport,
                            &record,
                            snow_core::Source::Cache {
                                last_refreshed_at: record.synced_at,
                            },
                            snow_core::Completeness::Partial {
                                reason: snow_core::PartialReason::NarrowedProjection,
                            },
                            false,
                        ) {
                            Ok(result) => JsonRpcResponse::ok(id, result),
                            Err(err) => internal_error(id, err),
                        },
                        Ok(None) if rule.mode == snow_core::cache::policy::CacheMode::CacheOnly => {
                            JsonRpcResponse::error(
                                id,
                                -32072,
                                "cache miss",
                                Some(json!({
                                    "code": "CACHE_MISS",
                                    "operation": "business_application_get",
                                    "object": "business_application"
                                })),
                            )
                        }
                        Ok(None) => match core_business_application_lookup(lookup) {
                            Ok(core_lookup) => {
                                let live_without_local_io =
                                    rule.mode == snow_core::cache::policy::CacheMode::Live;
                                match state
                                    .core
                                    .get_business_application_policy_live(
                                        core_lookup,
                                        !live_without_local_io,
                                    )
                                    .await
                                {
                                    Ok(Some(application)) => match business_application_get_result(
                                        transport,
                                        &application.record,
                                        snow_core::Source::Live,
                                        snow_core::Completeness::Complete,
                                        live_without_local_io,
                                    ) {
                                        Ok(result) => JsonRpcResponse::ok(id, result),
                                        Err(err) => internal_error(id, err),
                                    },
                                    Ok(None) => JsonRpcResponse::error(
                                        id,
                                        -32004,
                                        "business application not found",
                                        None,
                                    ),
                                    Err(err) => internal_error(id, err),
                                }
                            }
                            Err(err) => invalid_params(id, err),
                        },
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationGetFresh => {
            match extract_business_application_lookup_params(&request.params) {
                Ok(lookup) => match extract_business_application_hydration_options(&request.params)
                {
                    Ok(options) => {
                        let core_lookup = match lookup {
                            BusinessApplicationLookup::SysId(sys_id) => {
                                match snow_core::BusinessApplicationLookup::sys_id(sys_id) {
                                    Ok(lookup) => lookup,
                                    Err(err) => return invalid_params(id, err),
                                }
                            }
                            BusinessApplicationLookup::Name(name) => {
                                snow_core::BusinessApplicationLookup::exact_name(name)
                            }
                        };
                        match state
                            .core
                            .get_business_application_fresh(core_lookup, options.clone().into())
                            .await
                        {
                            Ok(Some(application)) => {
                                match transport.business_application(&application.record) {
                                    Ok(mut business_application) => {
                                        business_application.unresolved_references =
                                            business_application_diagnostics(
                                                &application.unresolved_references,
                                            );
                                        let record_dto = business_application.record.clone();
                                        JsonRpcResponse::ok(
                                            id,
                                            json!({
                                                "business_application": business_application,
                                                "record": record_dto,
                                                "markdown": render_snow_record(&application.record),
                                                "hydration": options,
                                            }),
                                        )
                                    }
                                    Err(err) => internal_error(id, err),
                                }
                            }
                            Ok(None) => JsonRpcResponse::error(
                                id,
                                -32004,
                                "business application not found",
                                None,
                            ),
                            Err(err) => internal_error(id, err),
                        }
                    }
                    Err(err) => invalid_params(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationSearch => {
            match extract_business_application_search_params(&request.params) {
                Ok((params, mut options)) => {
                    let rule = state
                        .cache_policy
                        .active()
                        .rule_for("business_application_search", "business_application");
                    let snapshot = if rule.mode == snow_core::cache::policy::CacheMode::Live {
                        Ok(None)
                    } else {
                        business_application_cache_snapshot(state.core.as_ref()).await
                    };
                    match snapshot {
                        Ok(Some((_, last_refreshed_at)))
                            if rule.mode == snow_core::cache::policy::CacheMode::CacheOnly
                                || rule.ttl.is_some_and(|ttl| {
                                    last_refreshed_at + ttl > chrono::Utc::now()
                                }) =>
                        {
                            match state
                                .core
                                .query_business_applications(
                                    cached_business_application_search_query(&params),
                                )
                                .await
                            {
                                Ok(records)
                                    if !records.is_empty()
                                        || rule.mode
                                            == snow_core::cache::policy::CacheMode::CacheOnly =>
                                {
                                    let mut applications = Vec::with_capacity(records.len());
                                    let mut record_dtos = Vec::with_capacity(records.len());
                                    for record in records {
                                        match transport.business_application(&record) {
                                            Ok(application) => {
                                                record_dtos.push(application.record.clone());
                                                applications.push(application);
                                            }
                                            Err(err) => return internal_error(id, err),
                                        }
                                    }
                                    options.persist = false;
                                    match operation_envelope_result(
                                        "business_application_search",
                                        snow_core::Source::Cache { last_refreshed_at },
                                        snow_core::Completeness::Partial {
                                            reason: snow_core::PartialReason::NarrowedProjection,
                                        },
                                        json!({
                                            "business_applications": applications,
                                            "records": record_dtos,
                                            "hydration": options,
                                        }),
                                    ) {
                                        Ok(result) => JsonRpcResponse::ok(id, result),
                                        Err(err) => internal_error(id, err),
                                    }
                                }
                                // Cache hit but zero results and mode allows live: fall through to
                                // live so a record that was never synced isn't silently missing.
                                Ok(_) => {
                                    let live_without_local_io =
                                        rule.mode == snow_core::cache::policy::CacheMode::Live;
                                    options.persist = !live_without_local_io;
                                    let limit = params.limit.unwrap_or(20);
                                    match state
                                        .core
                                        .search_business_applications_policy_live(
                                            params,
                                            options.persist,
                                        )
                                        .await
                                    {
                                        Ok(business_applications) => {
                                            let reached_limit =
                                                business_applications.len() == limit;
                                            let mut applications =
                                                Vec::with_capacity(business_applications.len());
                                            let mut record_dtos =
                                                Vec::with_capacity(business_applications.len());
                                            for application in business_applications {
                                                let dto = if live_without_local_io {
                                                    transport.live_business_application(
                                                        &application.record,
                                                    )
                                                } else {
                                                    transport
                                                        .business_application(&application.record)
                                                };
                                                match dto {
                                                    Ok(mut application_dto) => {
                                                        application_dto.unresolved_references =
                                                            business_application_diagnostics(
                                                                &application.unresolved_references,
                                                            );
                                                        record_dtos
                                                            .push(application_dto.record.clone());
                                                        applications.push(application_dto);
                                                    }
                                                    Err(err) => return internal_error(id, err),
                                                }
                                            }
                                            let completeness = if reached_limit {
                                                snow_core::Completeness::Partial {
                                                    reason:
                                                        snow_core::PartialReason::PageLimitReached,
                                                }
                                            } else {
                                                snow_core::Completeness::Complete
                                            };
                                            match operation_envelope_result(
                                                "business_application_search",
                                                snow_core::Source::Live,
                                                completeness,
                                                json!({
                                                    "business_applications": applications,
                                                    "records": record_dtos,
                                                    "hydration": options,
                                                }),
                                            ) {
                                                Ok(result) => JsonRpcResponse::ok(id, result),
                                                Err(err) => internal_error(id, err),
                                            }
                                        }
                                        Err(err) => internal_error(id, err),
                                    }
                                }
                                Err(err) => internal_error(id, err),
                            }
                        }
                        Ok(_) if rule.mode == snow_core::cache::policy::CacheMode::CacheOnly => {
                            business_application_cache_miss(id, "business_application_search")
                        }
                        Ok(_) => {
                            let live_without_local_io =
                                rule.mode == snow_core::cache::policy::CacheMode::Live;
                            options.persist = !live_without_local_io;
                            let limit = params.limit.unwrap_or(20);
                            match state
                                .core
                                .search_business_applications_policy_live(params, options.persist)
                                .await
                            {
                                Ok(business_applications) => {
                                    let reached_limit = business_applications.len() == limit;
                                    let mut applications =
                                        Vec::with_capacity(business_applications.len());
                                    let mut record_dtos =
                                        Vec::with_capacity(business_applications.len());
                                    for application in business_applications {
                                        let dto = if live_without_local_io {
                                            transport.live_business_application(&application.record)
                                        } else {
                                            transport.business_application(&application.record)
                                        };
                                        match dto {
                                            Ok(mut application_dto) => {
                                                application_dto.unresolved_references =
                                                    business_application_diagnostics(
                                                        &application.unresolved_references,
                                                    );
                                                record_dtos.push(application_dto.record.clone());
                                                applications.push(application_dto);
                                            }
                                            Err(err) => return internal_error(id, err),
                                        }
                                    }
                                    let completeness = if reached_limit {
                                        snow_core::Completeness::Partial {
                                            reason: snow_core::PartialReason::PageLimitReached,
                                        }
                                    } else {
                                        snow_core::Completeness::Complete
                                    };
                                    match operation_envelope_result(
                                        "business_application_search",
                                        snow_core::Source::Live,
                                        completeness,
                                        json!({
                                            "business_applications": applications,
                                            "records": record_dtos,
                                            "hydration": options,
                                        }),
                                    ) {
                                        Ok(result) => JsonRpcResponse::ok(id, result),
                                        Err(err) => internal_error(id, err),
                                    }
                                }
                                Err(err) => internal_error(id, err),
                            }
                        }
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationQuery => {
            match extract_business_application_query_params(&request.params) {
                Ok(params) => {
                    let rule = state
                        .cache_policy
                        .active()
                        .rule_for("business_application_query", "business_application");
                    let snapshot = if rule.mode == snow_core::cache::policy::CacheMode::Live {
                        Ok(None)
                    } else {
                        business_application_cache_snapshot(state.core.as_ref()).await
                    };
                    match snapshot {
                        Ok(Some((_, last_refreshed_at)))
                            if rule.mode == snow_core::cache::policy::CacheMode::CacheOnly
                                || rule.ttl.is_some_and(|ttl| {
                                    last_refreshed_at + ttl > chrono::Utc::now()
                                }) =>
                        {
                            match query_business_applications_local(state.core.as_ref(), &params)
                                .await
                            {
                                Ok(records) => {
                                    let mut applications = Vec::with_capacity(records.len());
                                    for record in records {
                                        match transport.business_application(&record) {
                                            Ok(application) => applications.push(application),
                                            Err(err) => return internal_error(id, err),
                                        }
                                    }
                                    match operation_envelope_result(
                                        "business_application_query",
                                        snow_core::Source::Cache { last_refreshed_at },
                                        snow_core::Completeness::Partial {
                                            reason: snow_core::PartialReason::NarrowedProjection,
                                        },
                                        json!({ "business_applications": applications }),
                                    ) {
                                        Ok(result) => JsonRpcResponse::ok(id, result),
                                        Err(err) => internal_error(id, err),
                                    }
                                }
                                Err(err) => internal_error(id, err),
                            }
                        }
                        Ok(_) if rule.mode == snow_core::cache::policy::CacheMode::CacheOnly => {
                            business_application_cache_miss(id, "business_application_query")
                        }
                        Ok(_) => {
                            let live_without_local_io =
                                rule.mode == snow_core::cache::policy::CacheMode::Live;
                            let query = match core_business_application_query(&params) {
                                Ok(query) => query,
                                Err(err) => return invalid_params(id, err),
                            };
                            let limit = query.limit.unwrap_or(20);
                            match state
                                .core
                                .query_business_applications_policy_live(
                                    query,
                                    !live_without_local_io,
                                )
                                .await
                            {
                                Ok(business_applications) => {
                                    let reached_limit = business_applications.len() == limit;
                                    let mut applications =
                                        Vec::with_capacity(business_applications.len());
                                    for application in business_applications {
                                        let dto = if live_without_local_io {
                                            transport.live_business_application(&application.record)
                                        } else {
                                            transport.business_application(&application.record)
                                        };
                                        match dto {
                                            Ok(application) => applications.push(application),
                                            Err(err) => return internal_error(id, err),
                                        }
                                    }
                                    let completeness = if reached_limit {
                                        snow_core::Completeness::Partial {
                                            reason: snow_core::PartialReason::PageLimitReached,
                                        }
                                    } else {
                                        snow_core::Completeness::Complete
                                    };
                                    match operation_envelope_result(
                                        "business_application_query",
                                        snow_core::Source::Live,
                                        completeness,
                                        json!({ "business_applications": applications }),
                                    ) {
                                        Ok(result) => JsonRpcResponse::ok(id, result),
                                        Err(err) => internal_error(id, err),
                                    }
                                }
                                Err(err) => internal_error(id, err),
                            }
                        }
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationServers => {
            // Deserialize directly into the canonical snow_core request contract
            // (which owns `deny_unknown_fields` and selector/bounds validation),
            // then validate up-front so bad params surface as `invalid_params`.
            // Validation is run here -- rather than relying on the inner
            // re-validation inside `SnowCore::business_application_servers` -- so a
            // validation failure maps to `-32602 invalid_params` instead of being
            // misreported as an internal/service error.
            match parse_business_application_servers_params(&request.params) {
                Ok(params) => {
                    let mut traversal_params = params.traversal;
                    traversal_params.persist = Some(params.persist);
                    traversal_params.prune_stale = params.prune_stale;
                    match business_application_servers(
                        state.core.as_ref(),
                        transport,
                        traversal_params,
                    )
                    .await
                    {
                        Ok(Some(result)) => JsonRpcResponse::ok(id, result),
                        Ok(None) => JsonRpcResponse::error(
                            id,
                            -32004,
                            "business application not found",
                            Some(json!({
                                "endpoint_status": "live_confirmation_not_attempted",
                                "relationship_status": "unknown_not_synced"
                            })),
                        ),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationServersCached => {
            match parse_business_application_servers_cached_params(&request.params) {
                Ok(params) => {
                    match business_application_servers_cached(
                        state.core.as_ref(),
                        transport,
                        params,
                    )
                    .await
                    {
                        Ok(Some(result)) => JsonRpcResponse::ok(id, result),
                        Ok(None) => JsonRpcResponse::error(
                            id,
                            -32004,
                            "business application not found",
                            None,
                        ),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationsForServer => {
            match parse_business_applications_for_server_params(&request.params) {
                Ok(params) => {
                    match business_applications_for_server(state.core.as_ref(), transport, params)
                        .await
                    {
                        Ok(Some(result)) => JsonRpcResponse::ok(id, result),
                        Ok(None) => JsonRpcResponse::error(
                            id,
                            -32004,
                            "server not found",
                            Some(json!({
                                "endpoint_status": "live_confirmation_not_attempted",
                                "relationship_status": "unknown_not_synced"
                            })),
                        ),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationSync => {
            match extract_business_application_sync_params(&request.params) {
                Ok(params) => {
                    if params.all {
                        return match state
                            .core
                            .sync_all_business_applications(params.options.into())
                            .await
                        {
                            Ok(summary) => JsonRpcResponse::ok(id, json!({ "summary": summary })),
                            Err(err) => internal_error(id, err),
                        };
                    }
                    match state
                        .core
                        .sync_business_applications(params.search_params, params.options.into())
                        .await
                    {
                        Ok(summary) => JsonRpcResponse::ok(id, json!({ "summary": summary })),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationFields => {
            match extract_business_application_fields_params(&request.params) {
                Ok(params) => {
                    match business_application_fields(state.core.as_ref(), params).await {
                        Ok(fields) => JsonRpcResponse::ok(id, json!({ "fields": fields })),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
