use super::super::*;

/// Convert parsed `--filter <field>:<op>:<value>` tokens into the daemon wire
/// shape, preserving their command-line order.
///
/// clap already validated each token (field/operator/value present, operator is
/// a known token) and preserved the order of the repeated `--filter` argument,
/// so this is a direct, total mapping with no re-parsing of the raw argv. The
/// emitted `BusinessApplicationQueryFilter` shape (field + operator + value) is
/// exactly what the daemon's `BusinessApplicationFieldFilter` expects on the
/// wire, so this change is transparent to the daemon.
pub(crate) fn business_app_filters_to_query(
    filters: Vec<BusinessAppFilter>,
) -> Vec<BusinessApplicationQueryFilter> {
    filters
        .into_iter()
        .map(|filter| BusinessApplicationQueryFilter {
            field: filter.field,
            operator: filter.operator,
            value: filter.value,
        })
        .collect()
}

pub(crate) async fn cmd_business_app_export_all(
    client: &DaemonRpcClient,
    format: business_app_export::BusinessAppExportFormat,
    output: PathBuf,
) -> Result<(), SnowError> {
    business_app_export::validate_output_parent(&output)?;
    let mut offset = 0;
    let mut records = Vec::new();
    let filters: &[BusinessApplicationQueryFilter] = &[];
    loop {
        let result = client
            .business_application_query_page_with_args(BusinessApplicationQueryPageArgs {
                query: BusinessApplicationQueryArgs {
                    text: None,
                    filters,
                    limit: Some(business_app_export::EXPORT_ALL_PAGE_SIZE),
                },
                offset: Some(offset),
            })
            .await?;
        let page_len =
            business_app_export::append_records_from_query_result(&mut records, &result)?;
        if page_len < business_app_export::EXPORT_ALL_PAGE_SIZE {
            break;
        }
        offset += page_len;
    }
    let result = serde_json::Value::Array(records);
    let bytes = business_app_export::serialize(&result, format)?;
    let exported_count = business_app_export::record_count(&result)?;
    business_app_export::write_file(&output, &bytes)?;
    println!(
        "Exported {exported_count} cached Business Applications to {}",
        output.display()
    );
    Ok(())
}

pub(crate) fn validate_business_app_export_all_options(
    text: Option<&str>,
    filter: &[BusinessAppFilter],
    limit: Option<usize>,
) -> Result<(), SnowError> {
    if text.is_some() || !filter.is_empty() || limit.is_some() {
        return Err(SnowError::Api(
            "business-app export --all accepts only --all, --format, and --output".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_business_app_sync_all_options(
    name: Option<&str>,
    operational_state_not: Option<&str>,
) -> Result<(), SnowError> {
    if name.is_some() || operational_state_not.is_some() {
        return Err(SnowError::Api(
            "business-app sync --all does not accept bounded sync filters".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn cmd_business_app_sync_all(
    client: &DaemonRpcClient,
    persist: bool,
    resolve_references: bool,
    reference_depth: Option<u32>,
    refresh_dictionary: bool,
    json: bool,
) -> Result<(), SnowError> {
    let summary = client
        .business_application_sync_with_args(BusinessApplicationSyncArgs {
            all: true,
            name: None,
            operational_state_not: None,
            persist,
            resolve_references,
            reference_depth,
            refresh_dictionary,
        })
        .await?;
    if json {
        print_full_dump_or_inline(&summary);
    } else {
        print!(
            "{}",
            display::format_business_application_summary_object(&summary)
        );
    }
    Ok(())
}

pub(crate) fn format_business_application_servers_result(result: &serde_json::Value) -> String {
    let payload = business_application_servers_payload(result);
    let summary = payload.get("relationship_summary");
    let servers = payload
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let server_count = summary
        .and_then(|summary| json_usize_from_paths(summary, &[&["servers_found"]]))
        .unwrap_or(servers.len());
    let max_depth = summary.and_then(|summary| json_display_from_paths(summary, &[&["max_depth"]]));
    let degraded_reasons = collect_business_application_servers_degraded_reasons(payload);

    // Fallback signals (present only when fallback_strategy != none).
    let fallback_used = summary
        .and_then(|summary| json_bool(summary, "fallback_used"))
        .unwrap_or(false);
    let fallback_requested = summary
        .and_then(|summary| summary.get("fallback_strategy"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|strategy| !strategy.is_empty() && strategy != "none");
    let cmdb_servers_found = summary.and_then(|summary| json_usize(summary, "cmdb_servers_found"));
    let fallback_group = summary
        .and_then(|summary| json_display_from_paths(summary, &[&["fallback_group_display_name"]]));

    let mut out = String::new();
    let app = payload
        .get("business_application")
        .map(format_business_application_servers_root)
        .unwrap_or_else(|| "-".to_string());
    let _ = writeln!(out, "Business Application: {app}");
    if fallback_used {
        let _ = writeln!(
            out,
            "Servers found: {server_count}  (via ci_owner_group fallback -- CMDB relationships unmapped)"
        );
        if let Some(group) = &fallback_group {
            let _ = writeln!(out, "Group: {group}");
        }
    } else {
        let _ = writeln!(out, "Servers found: {server_count}");
    }
    if let Some(max_depth) = &max_depth {
        let _ = writeln!(out, "Max depth: {max_depth}");
    }
    let _ = writeln!(
        out,
        "Completeness: {}",
        if degraded_reasons.is_empty() {
            "complete"
        } else {
            "partial"
        }
    );
    let _ = writeln!(
        out,
        "Degraded: {}",
        if degraded_reasons.is_empty() {
            "none".to_string()
        } else {
            degraded_reasons.join(", ")
        }
    );

    if servers.is_empty() {
        // Fallback requested but produced no servers: explain why so an empty
        // result is not mistaken for "no fallback attempted".
        if fallback_requested && !fallback_used {
            let _ = writeln!(out, "CMDB traversal: 0 servers found.");
            let _ = writeln!(
                out,
                "Fallback: ci_owner_group requested but BA has no u_ci_owner_group set."
            );
            return out;
        }
        if fallback_used {
            let _ = writeln!(out, "CMDB traversal: 0 servers found.");
            let _ = writeln!(
                out,
                "Fallback: ci_owner_group returned no servers for the BA's owner group."
            );
            return out;
        }
        match max_depth {
            Some(max_depth) => {
                let _ = writeln!(
                    out,
                    "No associated server CIs found within max depth {max_depth}."
                );
            }
            None => {
                let _ = writeln!(out, "No associated server CIs found.");
            }
        }
        return out;
    }

    for server in servers {
        let name = json_display_from_paths(
            server,
            &[
                &["name"],
                &["record", "short_description"],
                &["record", "number"],
                &["record", "sys_id"],
                &["sys_id"],
            ],
        )
        .unwrap_or_else(|| "-".to_string());
        let class_name = json_display_from_paths(server, &[&["class_name"], &["record", "table"]])
            .unwrap_or_else(|| "-".to_string());
        let ip_address =
            json_display_from_paths(server, &[&["ip_address"], &["fields", "ip_address"]])
                .unwrap_or_else(|| "-".to_string());
        let status = json_display_from_paths(
            server,
            &[
                &["operational_status"],
                &["fields", "operational_status"],
                &["fields", "install_status"],
            ],
        )
        .unwrap_or_else(|| "-".to_string());
        let _ = writeln!(out, "{name}  {class_name}  {ip_address}  {status}");
    }

    if fallback_used {
        let cmdb_count = cmdb_servers_found.unwrap_or(0);
        let _ = writeln!(
            out,
            "\nWarning: {cmdb_count} servers found via CMDB traversal. Results are from CI owner group\nfallback and may not reflect all servers supporting this application.\nCMDB relationships should be reviewed and populated."
        );
    }

    out
}

pub(crate) fn format_business_application_servers_cached_result(
    result: &serde_json::Value,
) -> String {
    let payload = business_application_servers_cached_payload(result);
    let servers = payload
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    let mut out = String::new();
    let app = payload
        .get("business_application")
        .map(format_business_application_servers_root)
        .unwrap_or_else(|| "-".to_string());
    let _ = writeln!(out, "Business Application: {app}");
    let _ = writeln!(out, "Cached servers found: {}", servers.len());

    if servers.is_empty() {
        let _ = writeln!(out, "No cached associated server CIs found.");
        return out;
    }

    for relationship in servers {
        let server = relationship.get("server").unwrap_or(relationship);
        let mut suffix = Vec::new();
        if let Some(depth) = json_usize(relationship, "min_depth") {
            suffix.push(format!("depth {depth}"));
        }
        if let Some(provenance) = json_display_from_paths(relationship, &[&["provenance"]]) {
            suffix.push(provenance);
        }
        if relationship
            .get("tombstoned_at")
            .is_some_and(|value| !value.is_null())
        {
            suffix.push("tombstoned".to_string());
        }
        let suffix = if suffix.is_empty() {
            String::new()
        } else {
            format!("  [{}]", suffix.join(", "))
        };
        let _ = writeln!(out, "{}{}", format_cached_server_row(server), suffix);
    }

    out
}

pub(crate) fn format_business_applications_for_server_result(result: &serde_json::Value) -> String {
    let payload = business_applications_for_server_payload(result);
    let servers = payload
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let app_count: usize = servers
        .iter()
        .map(|server| {
            server
                .get("business_applications")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len)
        })
        .sum();

    let mut out = String::new();
    let _ = writeln!(out, "Matched servers: {}", servers.len());
    let _ = writeln!(out, "Cached Business Applications found: {app_count}");

    if servers.is_empty() {
        let _ = writeln!(out, "No cached Server found.");
        return out;
    }

    for server_relationships in servers {
        let server = server_relationships
            .get("server")
            .unwrap_or(server_relationships);
        let _ = writeln!(out, "Server: {}", format_cached_server_identity(server));
        let applications = server_relationships
            .get("business_applications")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if applications.is_empty() {
            let _ = writeln!(out, "  No cached Business Applications found.");
            continue;
        }
        for relationship in applications {
            let app = relationship
                .get("business_application")
                .unwrap_or(relationship);
            let mut suffix = Vec::new();
            if let Some(depth) = json_usize(relationship, "min_depth") {
                suffix.push(format!("depth {depth}"));
            }
            if let Some(provenance) = json_display_from_paths(relationship, &[&["provenance"]]) {
                suffix.push(provenance);
            }
            if relationship
                .get("tombstoned_at")
                .is_some_and(|value| !value.is_null())
            {
                suffix.push("tombstoned".to_string());
            }
            let suffix = if suffix.is_empty() {
                String::new()
            } else {
                format!(" [{}]", suffix.join(", "))
            };
            let _ = writeln!(
                out,
                "  {}{}",
                format_business_application_servers_root(app),
                suffix
            );
        }
    }

    out
}

pub(crate) fn business_application_servers_payload(
    result: &serde_json::Value,
) -> &serde_json::Value {
    result.get("business_application_servers").unwrap_or(result)
}

pub(crate) fn business_application_servers_cached_payload(
    result: &serde_json::Value,
) -> &serde_json::Value {
    result
        .get("business_application_servers_cached")
        .unwrap_or(result)
}

pub(crate) fn business_applications_for_server_payload(
    result: &serde_json::Value,
) -> &serde_json::Value {
    result
        .get("business_applications_for_server")
        .unwrap_or(result)
}

pub(crate) fn format_business_application_servers_root(app: &serde_json::Value) -> String {
    let number = json_display_from_paths(app, &[&["number"], &["record", "number"]]);
    let name = json_display_from_paths(app, &[&["name"], &["record", "short_description"]]);
    match (number, name) {
        (Some(number), Some(name)) if number != name => format!("{number} {name}"),
        (Some(number), _) => number,
        (_, Some(name)) => name,
        _ => json_display_from_paths(app, &[&["sys_id"], &["record", "sys_id"]])
            .unwrap_or_else(|| "-".to_string()),
    }
}

pub(crate) fn format_cached_server_row(server: &serde_json::Value) -> String {
    let name = format_cached_server_identity(server);
    let class_name = json_display_from_paths(server, &[&["class_name"], &["record", "table"]])
        .unwrap_or_else(|| "-".to_string());
    let ip_address = json_display_from_paths(server, &[&["ip_address"], &["fields", "ip_address"]])
        .unwrap_or_else(|| "-".to_string());
    let status = json_display_from_paths(
        server,
        &[
            &["operational_status"],
            &["fields", "operational_status"],
            &["fields", "install_status"],
        ],
    )
    .unwrap_or_else(|| "-".to_string());
    format!("{name}  {class_name}  {ip_address}  {status}")
}

pub(crate) fn format_cached_server_identity(server: &serde_json::Value) -> String {
    json_display_from_paths(
        server,
        &[
            &["name"],
            &["record", "short_description"],
            &["record", "number"],
            &["record", "sys_id"],
            &["sys_id"],
        ],
    )
    .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn collect_business_application_servers_degraded_reasons(
    payload: &serde_json::Value,
) -> Vec<String> {
    let mut reasons = BTreeSet::new();
    if let Some(summary) = payload.get("relationship_summary") {
        if json_bool(summary, "depth_limit_reached").unwrap_or(false) {
            reasons.insert("depth_limit_reached".to_string());
        }
        if json_bool(summary, "truncated").unwrap_or(false) {
            reasons.insert("truncated".to_string());
        }
        if json_usize(summary, "truncated_count").unwrap_or(0) > 0 {
            reasons.insert("truncated_count".to_string());
        }
        if json_usize(summary, "acl_restricted_count").unwrap_or(0) > 0 {
            reasons.insert("acl_restricted".to_string());
        }
        collect_degraded_reason_values(summary.get("degraded_reasons"), &mut reasons);
    }
    if payload
        .get("diagnostics")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|diagnostics| !diagnostics.is_empty())
    {
        reasons.insert("diagnostics".to_string());
    }
    reasons.into_iter().collect()
}

pub(crate) fn collect_degraded_reason_values(
    value: Option<&serde_json::Value>,
    reasons: &mut BTreeSet<String>,
) {
    match value {
        Some(serde_json::Value::Array(values)) => {
            for value in values {
                if let Some(reason) = json_display_value(value) {
                    reasons.insert(reason);
                }
            }
        }
        Some(serde_json::Value::Object(map)) => {
            for (key, value) in map {
                let include = match value {
                    serde_json::Value::Bool(flag) => *flag,
                    serde_json::Value::Number(number) => number.as_u64().unwrap_or(0) > 0,
                    serde_json::Value::String(text) => !text.trim().is_empty(),
                    serde_json::Value::Array(values) => !values.is_empty(),
                    serde_json::Value::Object(values) => !values.is_empty(),
                    serde_json::Value::Null => false,
                };
                if include {
                    reasons.insert(key.clone());
                }
            }
        }
        _ => {}
    }
}

pub(crate) fn json_display_from_paths(
    value: &serde_json::Value,
    paths: &[&[&str]],
) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).and_then(json_display_value))
}

pub(crate) fn json_usize_from_paths(value: &serde_json::Value, paths: &[&[&str]]) -> Option<usize> {
    paths.iter().find_map(|path| {
        json_path(value, path).and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_u64().map(|value| value as usize),
            serde_json::Value::String(text) => text.trim().parse::<usize>().ok(),
            _ => None,
        })
    })
}

pub(crate) fn json_path<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

pub(crate) fn json_display_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(flag) => Some(flag.to_string()),
        serde_json::Value::Object(_) => value
            .get("display_value")
            .and_then(json_display_value)
            .or_else(|| value.get("value").and_then(json_display_value))
            .or_else(|| value.get("display_name").and_then(json_display_value)),
        _ => None,
    }
}

pub(crate) fn json_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
    value.get(key).and_then(serde_json::Value::as_bool)
}

pub(crate) fn json_usize(value: &serde_json::Value, key: &str) -> Option<usize> {
    value.get(key).and_then(|value| match value {
        serde_json::Value::Number(number) => number.as_u64().map(|value| value as usize),
        serde_json::Value::String(text) => text.trim().parse::<usize>().ok(),
        _ => None,
    })
}

/// Dispatch the `snow business-app` subcommand family over the daemon client.
pub(crate) async fn cmd_business_app(
    client: &DaemonRpcClient,
    action: BusinessAppCommand,
) -> Result<(), SnowError> {
    match action {
        BusinessAppCommand::Get {
            sys_id,
            name,
            fresh,
            json,
            full,
        } => {
            if sys_id.is_none() && name.is_none() {
                return Err(SnowError::Api(
                    "business-app get requires --sys-id or --name".to_string(),
                ));
            }
            let app = client
                .business_application_get(sys_id.as_deref(), name.as_deref(), fresh)
                .await?;
            match app {
                Some(app) => {
                    if json {
                        print_json(&app)?;
                    } else {
                        print!("{}", display::format_business_application(&app, full));
                    }
                }
                None => println!("Business Application not found."),
            }
            Ok(())
        }
        BusinessAppCommand::Search {
            name,
            operational_state_not,
            limit,
            json,
            full,
        } => {
            let apps = client
                .business_application_search(
                    name.as_deref(),
                    operational_state_not.as_deref(),
                    limit,
                )
                .await?;
            if json {
                return print_json(&apps);
            }
            if apps.is_empty() {
                println!("No Business Applications found.");
                return Ok(());
            }
            for app in &apps {
                if full {
                    print!("{}", display::format_business_application(app, true));
                } else {
                    println!("{}", display::format_business_application_summary(app));
                }
                println!();
            }
            Ok(())
        }
        BusinessAppCommand::Query {
            filter,
            limit,
            json,
        } => {
            if filter.is_empty() {
                return Err(SnowError::Api(
                    "business-app query requires at least one --filter".to_string(),
                ));
            }
            let filters = business_app_filters_to_query(filter);
            let result = client.business_application_query(&filters, limit).await?;
            if json {
                print_full_dump_or_inline(&result);
            } else {
                // Query returns records in a backend-owned shape; render generically.
                print_full_dump_or_inline(&result);
            }
            Ok(())
        }
        BusinessAppCommand::Servers {
            number,
            sys_id,
            cached,
            for_server,
            max_depth,
            max_cis,
            max_edges,
            max_service_membership_associations,
            max_service_membership_pages,
            relationship_type,
            include_paths,
            fallback_strategy,
            no_persist,
            prune_stale,
            include_tombstoned,
            json,
        } => {
            let ba_selector_count = [number.is_some(), sys_id.is_some()]
                .into_iter()
                .filter(|selected| *selected)
                .count();
            if for_server.is_some() {
                if ba_selector_count != 0
                    || cached
                    || max_depth.is_some()
                    || max_cis.is_some()
                    || max_edges.is_some()
                    || max_service_membership_associations.is_some()
                    || max_service_membership_pages.is_some()
                    || !relationship_type.is_empty()
                    || include_paths
                    || no_persist
                    || prune_stale
                {
                    return Err(SnowError::Api(
                        "business-app servers --for-server cannot combine with BA selectors or live traversal flags".to_string(),
                    ));
                }
                let result = client
                    .business_applications_for_server(BusinessApplicationsForServerArgs {
                        sys_id: for_server.as_deref(),
                        include_tombstoned,
                    })
                    .await?;
                if json {
                    print_full_dump_or_inline(&result);
                } else {
                    print!(
                        "{}",
                        format_business_applications_for_server_result(&result)
                    );
                }
                return Ok(());
            }
            if cached {
                if ba_selector_count != 1 {
                    return Err(SnowError::Api(
                        "business-app servers --cached requires exactly one of --number or --sys-id"
                            .to_string(),
                    ));
                }
                let result = client
                    .business_application_servers_cached(BusinessApplicationServersCachedArgs {
                        number: number.as_deref(),
                        sys_id: sys_id.as_deref(),
                        include_tombstoned,
                    })
                    .await?;
                if json {
                    print_full_dump_or_inline(&result);
                } else {
                    print!(
                        "{}",
                        format_business_application_servers_cached_result(&result)
                    );
                }
                return Ok(());
            }
            if ba_selector_count != 1 {
                return Err(SnowError::Api(
                    "business-app servers requires exactly one of --number, --sys-id, or --for-server"
                        .to_string(),
                ));
            }
            if prune_stale && no_persist {
                return Err(SnowError::Api(
                    "business-app servers --prune-stale requires a persisting live traversal"
                        .to_string(),
                ));
            }
            if include_tombstoned {
                return Err(SnowError::Api(
                    "business-app servers --include-tombstoned is only valid with --cached or --for-server"
                        .to_string(),
                ));
            }
            let result = client
                .business_application_servers(BusinessApplicationServersArgs {
                    number: number.as_deref(),
                    sys_id: sys_id.as_deref(),
                    max_depth,
                    max_cis,
                    max_edges,
                    max_service_membership_associations,
                    max_service_membership_pages,
                    relationship_type: &relationship_type,
                    include_paths,
                    fallback_strategy: fallback_strategy.as_wire(),
                    persist: !no_persist,
                    prune_stale,
                })
                .await?;
            if json {
                print_full_dump_or_inline(&result);
            } else {
                print!("{}", format_business_application_servers_result(&result));
            }
            Ok(())
        }
        BusinessAppCommand::Export {
            all,
            format,
            output,
            text,
            filter,
            limit,
        } => {
            let export_format = match format {
                cli::BusinessAppExportFormat::Json => {
                    business_app_export::BusinessAppExportFormat::Json
                }
                cli::BusinessAppExportFormat::Jsonl => {
                    business_app_export::BusinessAppExportFormat::Jsonl
                }
                cli::BusinessAppExportFormat::Csv => {
                    business_app_export::BusinessAppExportFormat::Csv
                }
            };
            if all {
                validate_business_app_export_all_options(text.as_deref(), &filter, limit)?;
                return cmd_business_app_export_all(client, export_format, output).await;
            }
            business_app_export::validate_limit(limit)?;
            business_app_export::validate_output_parent(&output)?;
            business_app_export::validate_text(text.as_deref())?;
            let filters = business_app_filters_to_query(filter);
            let result = client
                .business_application_query_with_args(BusinessApplicationQueryArgs {
                    text: text.as_deref(),
                    filters: &filters,
                    limit,
                })
                .await?;
            let bytes = business_app_export::serialize(&result, export_format)?;
            let exported_count = business_app_export::record_count(&result)?;
            business_app_export::write_file(&output, &bytes)?;
            println!(
                "Exported {exported_count} Business Applications to {}",
                output.display()
            );
            Ok(())
        }
        BusinessAppCommand::Fields { refresh, json } => {
            let fields = client.business_application_fields(refresh).await?;
            if json {
                print_full_dump_or_inline(&fields);
            } else {
                print!("{}", display::format_business_application_fields(&fields));
            }
            Ok(())
        }
        BusinessAppCommand::Sync {
            all,
            name,
            operational_state_not,
            persist,
            resolve_references,
            reference_depth,
            refresh_dictionary,
            json,
        } => {
            if all {
                validate_business_app_sync_all_options(
                    name.as_deref(),
                    operational_state_not.as_deref(),
                )?;
                return cmd_business_app_sync_all(
                    client,
                    persist,
                    resolve_references,
                    reference_depth,
                    refresh_dictionary,
                    json,
                )
                .await;
            }
            let summary = client
                .business_application_sync(
                    name.as_deref(),
                    operational_state_not.as_deref(),
                    persist,
                    resolve_references,
                    reference_depth,
                    refresh_dictionary,
                )
                .await?;
            if json {
                print_full_dump_or_inline(&summary);
            } else {
                print!(
                    "{}",
                    display::format_business_application_summary_object(&summary)
                );
            }
            Ok(())
        }
    }
}

pub(crate) mod business_app_export {
    use super::SnowError;
    use serde_json::Value;
    use std::collections::BTreeSet;
    use std::io::Write;
    use std::path::Path;

    const MAX_EXPORT_LIMIT: usize = 500;
    pub(crate) const EXPORT_ALL_PAGE_SIZE: usize = 500;
    const CSV_BASE_HEADERS: &[&str] = &[
        "record.sys_id",
        "record.number",
        "name",
        "record.short_description",
        "operational_state",
        "business_owner",
        "is_owner",
        "ci_owner_group",
        "primary_support_group",
        "primary_portfolio",
        "attested_date",
        "vault_relative_path",
        "browser_url",
    ];
    const CSV_BASE_SOURCE_FIELDS: &[&str] = &[
        "sys_id",
        "number",
        "name",
        "short_description",
        "operational_status",
        "business_owner",
        "it_application_owner",
        "managed_by_group",
        "support_group",
        "portfolio",
        "attested_date",
        "vault_relative_path",
        "browser_url",
    ];

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum BusinessAppExportFormat {
        Json,
        Jsonl,
        Csv,
    }

    pub(crate) fn validate_limit(limit: Option<usize>) -> Result<(), SnowError> {
        match limit {
            Some(0) => Err(SnowError::Api("`limit` must be at least 1".to_string())),
            Some(value) if value > MAX_EXPORT_LIMIT => Err(SnowError::Api(format!(
                "`limit` must be at most {MAX_EXPORT_LIMIT}"
            ))),
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_output_parent(output: &Path) -> Result<(), SnowError> {
        if output.file_name().is_none() {
            return Err(SnowError::Api(
                "business-app export --output must name a file".to_string(),
            ));
        }
        if output.is_dir() {
            return Err(SnowError::Api(format!(
                "business-app export --output must name a file, got directory: {}",
                output.display()
            )));
        }
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.exists() {
            return Err(SnowError::Api(format!(
                "business-app export output parent does not exist: {}",
                parent.display()
            )));
        }
        if !parent.is_dir() {
            return Err(SnowError::Api(format!(
                "business-app export output parent is not a directory: {}",
                parent.display()
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_text(text: Option<&str>) -> Result<(), SnowError> {
        if let Some(text) = text
            && text.trim().is_empty()
        {
            return Err(SnowError::Api(
                "business-app export --text must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn serialize(
        result: &Value,
        format: BusinessAppExportFormat,
    ) -> Result<Vec<u8>, SnowError> {
        let records = records_from_query_result(result)?;
        validate_record_objects(records)?;
        match format {
            BusinessAppExportFormat::Json => {
                serde_json::to_vec_pretty(&Value::Array(records.to_vec()))
                    .map_err(|err| SnowError::Api(err.to_string()))
            }
            BusinessAppExportFormat::Jsonl => serialize_jsonl(records),
            BusinessAppExportFormat::Csv => serialize_csv(records),
        }
    }

    pub(crate) fn record_count(result: &Value) -> Result<usize, SnowError> {
        Ok(records_from_query_result(result)?.len())
    }

    pub(crate) fn append_records_from_query_result(
        target: &mut Vec<Value>,
        result: &Value,
    ) -> Result<usize, SnowError> {
        let records = records_from_query_result(result)?;
        validate_record_objects(records)?;
        let count = records.len();
        target.extend(records.iter().cloned());
        Ok(count)
    }

    pub(crate) fn write_file(output: &Path, bytes: &[u8]) -> Result<(), SnowError> {
        validate_output_parent(output)?;
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let file_name = output.file_name().ok_or_else(|| {
            SnowError::Api("business-app export --output must name a file".to_string())
        })?;
        let temp_name = format!(
            ".{}.{}.tmp",
            file_name.to_string_lossy(),
            uuid::Uuid::new_v4()
        );
        let temp_path = parent.join(temp_name);
        let write_result = (|| -> Result<(), SnowError> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();

        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(err);
        }

        if let Err(err) = std::fs::rename(&temp_path, output) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(SnowError::from(err));
        }
        Ok(())
    }

    fn records_from_query_result(result: &Value) -> Result<&[Value], SnowError> {
        match result {
            Value::Array(records) => Ok(records.as_slice()),
            Value::Object(map) => {
                for key in ["business_applications", "records", "results"] {
                    if let Some(value) = map.get(key) {
                        return value.as_array().map(Vec::as_slice).ok_or_else(|| {
                            SnowError::Api(format!(
                                "business_application_query field `{key}` was not an array"
                            ))
                        });
                    }
                }
                Err(SnowError::Api(
                    "business_application_query result did not contain an exportable array"
                        .to_string(),
                ))
            }
            _ => Err(SnowError::Api(
                "business_application_query result was not an exportable array".to_string(),
            )),
        }
    }

    fn validate_record_objects(records: &[Value]) -> Result<(), SnowError> {
        if let Some((index, _)) = records
            .iter()
            .enumerate()
            .find(|(_, record)| !record.is_object())
        {
            return Err(SnowError::Api(format!(
                "business_application_query record at index {index} was not an object"
            )));
        }
        Ok(())
    }

    fn serialize_jsonl(records: &[Value]) -> Result<Vec<u8>, SnowError> {
        let mut output = Vec::new();
        for record in records {
            serde_json::to_writer(&mut output, record)
                .map_err(|err| SnowError::Api(err.to_string()))?;
            output.push(b'\n');
        }
        Ok(output)
    }

    fn serialize_csv(records: &[Value]) -> Result<Vec<u8>, SnowError> {
        let headers = csv_headers(records);
        let mut output = String::new();
        write_csv_row(&mut output, &headers);
        for record in records {
            let row = headers
                .iter()
                .map(|header| csv_cell_for_header(record, header))
                .collect::<Vec<_>>();
            write_csv_row(&mut output, &row);
        }
        Ok(output.into_bytes())
    }

    fn csv_headers(records: &[Value]) -> Vec<String> {
        let mut headers = CSV_BASE_HEADERS
            .iter()
            .map(|header| (*header).to_string())
            .collect::<Vec<_>>();
        let mut projected_fields = BTreeSet::new();
        for record in records {
            if let Some(fields) = record.get("fields").and_then(Value::as_object) {
                for field in fields.keys() {
                    if !CSV_BASE_HEADERS.contains(&field.as_str())
                        && !CSV_BASE_SOURCE_FIELDS.contains(&field.as_str())
                    {
                        projected_fields.insert(field.clone());
                    }
                }
            }
        }
        headers.extend(projected_fields);
        headers
    }

    fn csv_cell_for_header(record: &Value, header: &str) -> String {
        match header {
            "record.sys_id" => value_text(record.get("record").and_then(|v| v.get("sys_id"))),
            "record.number" => value_text(record.get("record").and_then(|v| v.get("number"))),
            "name" => value_text(record.get("name")),
            "record.short_description" => value_text(
                record
                    .get("record")
                    .and_then(|v| v.get("short_description")),
            ),
            "operational_state" => value_text(record.get("operational_state")),
            "business_owner" => value_text(record.get("business_owner")),
            "is_owner" => value_text(record.get("is_owner")),
            "ci_owner_group" => value_text(record.get("ci_owner_group")),
            "primary_support_group" => value_text(record.get("primary_support_group")),
            "primary_portfolio" => value_text(record.get("primary_portfolio")),
            "attested_date" => value_text(record.get("attested_date")),
            "vault_relative_path" => value_text(record.get("vault_relative_path")),
            "browser_url" => value_text(record.get("browser_url")),
            field => value_text(
                record
                    .get("fields")
                    .and_then(Value::as_object)
                    .and_then(|fields| fields.get(field)),
            ),
        }
    }

    fn value_text(value: Option<&Value>) -> String {
        let Some(value) = value else {
            return String::new();
        };
        match value {
            Value::Null => String::new(),
            Value::String(text) => text.clone(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::Array(_) => compact_json(value),
            Value::Object(map) => {
                for key in ["display_name", "display_value", "value"] {
                    if let Some(text) = map
                        .get(key)
                        .and_then(Value::as_str)
                        .filter(|text| !text.trim().is_empty())
                    {
                        return text.to_string();
                    }
                    if key == "value"
                        && let Some(raw_value) = map.get(key)
                        && !raw_value.is_null()
                    {
                        return value_text(Some(raw_value));
                    }
                }
                compact_json(value)
            }
        }
    }

    fn compact_json(value: &Value) -> String {
        serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
    }

    fn write_csv_row(output: &mut String, cells: &[String]) {
        for (index, cell) in cells.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            write_csv_cell(output, cell);
        }
        output.push('\n');
    }

    fn write_csv_cell(output: &mut String, cell: &str) {
        if cell.chars().any(|ch| matches!(ch, ',' | '"' | '\n' | '\r')) {
            output.push('"');
            for ch in cell.chars() {
                if ch == '"' {
                    output.push_str("\"\"");
                } else {
                    output.push(ch);
                }
            }
            output.push('"');
        } else {
            output.push_str(cell);
        }
    }
}
