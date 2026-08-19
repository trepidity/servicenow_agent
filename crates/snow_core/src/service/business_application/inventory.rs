use super::*;

impl BusinessApplicationService {
    pub async fn business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        let options = params.validate()?;
        // Public callers express "use the default relationship-type set" by
        // leaving `relationship_type` empty; honor that by resolving the default
        // labels to stable identities when empty.
        self.business_application_servers_with_options(options, true)
            .await
    }

    pub async fn get_business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        self.business_application_servers(params).await
    }
}

impl BusinessApplicationService {
    /// Core graph traversal for [`Self::business_application_servers`].
    ///
    /// `defaults_when_empty` controls how an empty `options.relationship_type`
    /// is interpreted:
    /// - `true` (the public path): an empty allowlist means "use the default
    ///   relationship-type set", which is resolved to stable `cmdb_rel_type`
    ///   sys_ids once at the start of the traversal (see
    ///   [`Self::resolve_relationship_type_allowlist`]).
    /// - `false`: an empty allowlist is taken literally as "match all
    ///   relationship types" (no filtering, no resolution query).
    ///
    /// An explicitly-supplied non-empty allowlist is always used verbatim and is
    /// matched by both raw value (sys_id) and display label, preserving the
    /// ability for callers to pass either form.
    pub(super) async fn business_application_servers_with_options(
        &self,
        options: BusinessApplicationServersOptions,
        defaults_when_empty: bool,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        let Some(application) = self
            .resolve_business_application_servers_selector(&options)
            .await?
        else {
            return Ok(None);
        };

        // Resolve the effective allowlist to stable relationship-type identities
        // (sys_ids) before traversal. Matching on sys_ids instead of mutable
        // display labels means a renamed/localized `cmdb_rel_type` label no
        // longer silently drops every edge.
        let allowed_relationship_types = self
            .resolve_relationship_type_allowlist(&options, defaults_when_empty)
            .await?;
        let service_discovery_relationship_types =
            BUSINESS_APPLICATION_SERVICE_DISCOVERY_RELATIONSHIP_TYPES
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>();
        let run_started_at = Utc::now();
        let mut summary = BusinessApplicationServersSummary::new(&options);
        let mut diagnostics = Vec::new();
        let mut visited = HashSet::from([application.record.sys_id.clone()]);
        let mut frontier = vec![application.record.sys_id.clone()];
        let mut ci_classes = HashMap::from([(
            application.record.sys_id.clone(),
            BUSINESS_APPLICATION_TABLE.to_string(),
        )]);
        // Maps a CI sys_id to every recorded edge-chain (route) from the root BA
        // to that CI. A simple tree gives each CI one chain; in a diamond a CI is
        // reachable via multiple parents, so we keep a Vec of alternate chains
        // (only grown beyond one when `include_paths` requests full route
        // reporting). The root has a single empty chain.
        let mut paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
            HashMap::from([(application.record.sys_id.clone(), vec![Vec::new()])]);
        let mut service_paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
            HashMap::from([(application.record.sys_id.clone(), vec![Vec::new()])]);
        let mut service_membership_seed_sys_ids = HashSet::new();
        let mut servers_by_sys_id: BTreeMap<String, Server> = BTreeMap::new();
        let mut server_paths: BTreeMap<String, Vec<BusinessApplicationServerPath>> =
            BTreeMap::new();
        // Per-traversal memo of the hierarchy-backed server-class decision, keyed
        // by `sys_class_name`. The cheap sync checks never reach here; this only
        // caches the result of the `sys_db_object` super_class descent so the same
        // unrecognized class is not re-queried across BFS levels.
        let mut server_class_cache: HashMap<String, bool> = HashMap::new();

        for depth in 1..=options.max_depth {
            if frontier.is_empty() || summary.edge_limit_reached || summary.ci_limit_reached {
                break;
            }

            let relationships = self
                .business_application_relationship_level(
                    &frontier,
                    &options,
                    &mut summary,
                    &mut diagnostics,
                )
                .await?;
            let frontier_set = frontier.iter().cloned().collect::<HashSet<_>>();
            let mut newly_seen = Vec::new();

            for relationship in relationships {
                if let Some(class_name) = relationship.parent_class.as_deref() {
                    ci_classes
                        .entry(relationship.parent_sys_id.clone())
                        .or_insert_with(|| class_name.to_string());
                }
                if let Some(class_name) = relationship.child_class.as_deref() {
                    ci_classes
                        .entry(relationship.child_sys_id.clone())
                        .or_insert_with(|| class_name.to_string());
                }

                if depth == 1
                    && relationship
                        .relationship_type
                        .matches_any(&service_discovery_relationship_types)
                    && let Some((current_sys_id, adjacent_sys_id, direction)) =
                        relationship.traversal_endpoint(&frontier_set)
                {
                    let adjacent_class = if adjacent_sys_id == relationship.parent_sys_id {
                        relationship.parent_class.as_deref()
                    } else {
                        relationship.child_class.as_deref()
                    };
                    if is_application_service_class(adjacent_class) {
                        let edge = relationship.path_edge(depth, direction);
                        if extend_path_chains(
                            &mut service_paths_by_ci,
                            &current_sys_id,
                            &adjacent_sys_id,
                            edge,
                        ) {
                            service_membership_seed_sys_ids.insert(adjacent_sys_id.clone());
                        } else {
                            summary.cycle_count += 1;
                            push_business_application_server_diagnostic(
                                &mut summary,
                                &mut diagnostics,
                                "cmdb_rel_ci",
                                CMDB_REL_CI_TABLE,
                                &adjacent_sys_id,
                                ReferenceResolutionReason::CycleDetected,
                                "service discovery skipped a mixed-direction route",
                            );
                        }
                    }
                }

                if !relationship
                    .relationship_type
                    .matches_any(&allowed_relationship_types)
                {
                    continue;
                }

                let Some((current_sys_id, adjacent_sys_id, direction)) =
                    relationship.traversal_endpoint(&frontier_set)
                else {
                    push_business_application_server_diagnostic(
                        &mut summary,
                        &mut diagnostics,
                        "cmdb_rel_ci",
                        CMDB_REL_CI_TABLE,
                        "",
                        ReferenceResolutionReason::ReferenceResolutionFailed,
                        "relationship did not include a current frontier endpoint",
                    );
                    continue;
                };

                if visited.contains(&adjacent_sys_id) {
                    // The adjacent CI was already discovered. Two cases:
                    //  (a) true cycle/back-edge: the adjacent CI is an ancestor
                    //      of the current CI along some recorded route, so this
                    //      edge points back up the graph; or
                    //  (b) alternate forward path (diamond): the adjacent CI is
                    //      reachable via a different parent than before.
                    // Case (b) is a genuine additional route to the same CI and,
                    // when `include_paths` is set, must be recorded so the plural
                    // `server_paths` reflects every route. Neither case re-enqueues
                    // the CI for traversal (it is already visited).
                    let is_back_edge = path_chains_contain_ancestor(
                        paths_by_ci.get(&current_sys_id),
                        &adjacent_sys_id,
                        &current_sys_id,
                    );
                    if is_back_edge {
                        summary.cycle_count += 1;
                        push_business_application_server_diagnostic(
                            &mut summary,
                            &mut diagnostics,
                            "cmdb_ci",
                            CMDB_CI_TABLE,
                            &adjacent_sys_id,
                            ReferenceResolutionReason::CycleDetected,
                            "relationship traversal skipped a CI that was already visited",
                        );
                    } else if options.include_paths || options.persist {
                        // Record the alternate route(s) into the adjacent CI by
                        // extending each of the current CI's chains with this edge.
                        let edge = relationship.path_edge(depth, direction.clone());
                        let extended = extend_path_chains(
                            &mut paths_by_ci,
                            &current_sys_id,
                            &adjacent_sys_id,
                            edge,
                        );
                        if extended {
                            // A server reached again via an alternate route needs its
                            // new route emitted now, since it will not pass through the
                            // per-depth server-hydration loop again.
                            if options.include_paths {
                                emit_alternate_server_path(
                                    &servers_by_sys_id,
                                    &paths_by_ci,
                                    &mut server_paths,
                                    &adjacent_sys_id,
                                );
                            }
                        } else {
                            summary.cycle_count += 1;
                            push_business_application_server_diagnostic(
                                &mut summary,
                                &mut diagnostics,
                                "cmdb_rel_ci",
                                CMDB_REL_CI_TABLE,
                                &adjacent_sys_id,
                                ReferenceResolutionReason::CycleDetected,
                                "relationship traversal skipped a mixed-direction alternate route",
                            );
                        }
                    } else {
                        // include_paths off: an alternate route adds no result, so
                        // it is neither a traversal target nor recorded.
                        summary.cycle_count += 1;
                    }
                    continue;
                }
                // `max_cis` bounds the CIs examined BEYOND the root BA. `visited`
                // is seeded with the root, so the non-root examined count is
                // `visited.len() - 1`. Truncate once that budget is exhausted, so
                // a caller passing the exact expected non-root count is not
                // silently short by one.
                if visited.len().saturating_sub(1) >= options.max_cis {
                    summary.ci_limit_reached = true;
                    summary.mark_truncated(1);
                    push_business_application_server_diagnostic(
                        &mut summary,
                        &mut diagnostics,
                        "cmdb_ci",
                        CMDB_CI_TABLE,
                        &adjacent_sys_id,
                        ReferenceResolutionReason::FanoutLimitExceeded,
                        "max_cis prevented expanding another CI",
                    );
                    continue;
                }

                let edge = relationship.path_edge(depth, direction);
                // First discovery: seed the adjacent CI's routes from the current
                // CI's routes extended by this edge.
                if !extend_path_chains(&mut paths_by_ci, &current_sys_id, &adjacent_sys_id, edge) {
                    summary.cycle_count += 1;
                    push_business_application_server_diagnostic(
                        &mut summary,
                        &mut diagnostics,
                        "cmdb_rel_ci",
                        CMDB_REL_CI_TABLE,
                        &adjacent_sys_id,
                        ReferenceResolutionReason::CycleDetected,
                        "relationship traversal skipped a mixed-direction route",
                    );
                    continue;
                }
                visited.insert(adjacent_sys_id.clone());
                newly_seen.push(adjacent_sys_id);
            }

            self.business_application_hydrate_ci_classes(
                &newly_seen,
                &mut ci_classes,
                &mut summary,
                &mut diagnostics,
            )
            .await?;

            let mut server_ids = Vec::new();
            let mut next_frontier = Vec::new();
            for sys_id in newly_seen {
                let Some(class_name) = ci_classes.get(&sys_id).cloned() else {
                    continue;
                };
                // Hierarchy-aware classification: any descendant of
                // `cmdb_ci_server` (not just the narrow alias list) is collected
                // as a server. Cheap exact/alias/naming checks run first (no
                // network); only genuinely unrecognized classes fall back to the
                // metadata-backed super_class descent. Non-server CIs continue
                // into the next BFS frontier.
                let is_server = self
                    .business_application_class_is_server(
                        &class_name,
                        &mut server_class_cache,
                        &mut summary,
                        &mut diagnostics,
                    )
                    .await?;
                if is_server {
                    server_ids.push(sys_id);
                } else {
                    next_frontier.push(sys_id);
                }
            }

            let servers = self
                .business_application_hydrate_servers(&server_ids, &mut summary, &mut diagnostics)
                .await?;
            for server in servers {
                let sys_id = server.record.sys_id.clone();
                // Record this server first so later-depth alternate-route
                // discoveries can emit additional paths for it.
                servers_by_sys_id.entry(sys_id.clone()).or_insert(server);
                if options.include_paths
                    && let Some(chains) = paths_by_ci.get(&sys_id)
                {
                    // Emit every recorded route to this server. In a diamond the
                    // server already carries multiple chains by the time it is
                    // hydrated.
                    let entry = server_paths.entry(sys_id.clone()).or_default();
                    for chain in chains {
                        entry.push(BusinessApplicationServerPath {
                            edges: chain.clone(),
                        });
                    }
                }
            }

            if depth == options.max_depth && !next_frontier.is_empty() {
                summary.depth_limit_reached = true;
                summary.mark_truncated(next_frontier.len());
                push_business_application_server_diagnostic(
                    &mut summary,
                    &mut diagnostics,
                    "cmdb_rel_ci",
                    CMDB_REL_CI_TABLE,
                    "",
                    ReferenceResolutionReason::FanoutLimitExceeded,
                    "max_depth prevented expanding the next BFS frontier",
                );
                break;
            }

            frontier = next_frontier;
        }

        summary.cis_examined = visited.len();
        let mut discoveries: BTreeMap<String, BusinessApplicationServerDiscovery> = BTreeMap::new();
        for server in servers_by_sys_id.into_values() {
            let paths =
                business_application_server_paths_for(&paths_by_ci, server.record.sys_id.as_str());
            merge_business_application_server_discovery(
                &mut discoveries,
                server,
                BusinessApplicationServerProvenance::Relationship,
                paths,
            );
        }

        let service_membership_servers = self
            .business_application_service_membership_servers(
                &service_membership_seed_sys_ids,
                &service_paths_by_ci,
                &options,
                &mut summary,
                &mut diagnostics,
                &mut server_class_cache,
            )
            .await?;
        for (server, paths) in service_membership_servers {
            merge_business_application_server_discovery(
                &mut discoveries,
                server,
                BusinessApplicationServerProvenance::ServiceMembership,
                paths,
            );
        }

        summary.servers_found = discoveries.len();
        let mut servers = Vec::with_capacity(discoveries.len());
        let mut server_provenance = BTreeMap::new();
        let mut persisted_server_paths = BTreeMap::new();
        let mut server_paths = BTreeMap::new();
        for (sys_id, discovery) in discoveries {
            server_provenance.insert(sys_id.clone(), discovery.provenance);
            let paths = discovery.paths();
            if !paths.is_empty() {
                persisted_server_paths.insert(sys_id.clone(), paths.clone());
            }
            if options.include_paths && !paths.is_empty() {
                server_paths.insert(sys_id.clone(), paths);
            }
            servers.push(discovery.server);
        }
        if options.persist {
            self.persist_business_application_server_traversal(
                &application.record,
                &servers,
                &server_provenance,
                &persisted_server_paths,
                run_started_at,
                &options,
                &mut summary,
            )?;
        }

        // CMDB-gap fallback. Runs strictly AFTER persistence so that
        // fallback-discovered servers are never written to the durable BA↔server
        // membership or inventory-health tables: the persisting call above sees
        // only the traversal `servers`/`server_provenance`, and a 0-server
        // traversal persists 0 membership rows regardless of what the fallback
        // returns. Fallback results are appended to the live response below and
        // tagged via `server_sources`; they are live-only by construction.
        let mut server_sources: BTreeMap<String, ServerResultSource> = BTreeMap::new();
        if options.fallback_strategy.is_enabled() {
            // Pre-fallback traversal count. Always present (even when the
            // fallback does not fire) once a strategy is requested.
            summary.cmdb_servers_found = Some(summary.servers_found);
            summary.fallback_strategy = Some(options.fallback_strategy.as_str().to_string());
        }
        if options.fallback_strategy == FallbackStrategy::CiOwnerGroup && summary.servers_found == 0
        {
            let fallback_servers = self
                .business_application_ci_owner_group_fallback(
                    &application,
                    &options,
                    &mut summary,
                    &mut diagnostics,
                    &mut server_class_cache,
                )
                .await?;
            for server in fallback_servers {
                let sys_id = server.record.sys_id.clone();
                server_sources.insert(sys_id, ServerResultSource::CiOwnerGroupFallback);
                servers.push(server);
            }
            // `servers_found` reflects the TOTAL servers returned (traversal +
            // fallback). In the fallback case the traversal count is 0, so this
            // equals the fallback count while `cmdb_servers_found` stays 0.
            summary.servers_found = servers.len();
        }

        let inventory_health = if options.persist {
            self.ctx
                .query
                .store()
                .get_business_application_server_inventory_health(&application.record.sys_id)?
                .map(|row| BusinessApplicationServerInventoryHealth {
                    ba_sys_id: row.ba_sys_id,
                    run_started_at: row.run_started_at,
                    run_completed_at: row.run_completed_at,
                    service_membership_status: row.service_membership_status,
                    relationship_status: row.relationship_status,
                    inventory_status: row.inventory_status,
                    summary: serde_json::from_str(&row.summary_json)
                        .unwrap_or_else(|_| Value::Object(serde_json::Map::new())),
                })
        } else {
            None
        };

        Ok(Some(BusinessApplicationServersResult {
            business_application: BusinessApplicationServerApplication::from(&application),
            servers,
            server_sources,
            server_provenance,
            inventory_health,
            relationship_summary: summary,
            diagnostics,
            server_paths,
        }))
    }

    pub(super) fn business_application_relationship_health_status(
        summary: &BusinessApplicationServersSummary,
    ) -> &'static str {
        if summary.depth_limit_reached {
            "depth_limited"
        } else if summary.edge_limit_reached {
            "edge_budget_exhausted"
        } else if summary.ci_limit_reached {
            "ci_budget_exhausted"
        } else if summary.relationship_acl_restricted_count > 0 {
            "acl_restricted"
        } else if summary.truncated {
            "truncated"
        } else {
            "ok"
        }
    }

    pub(super) fn business_application_service_membership_health_status(
        summary: &BusinessApplicationServersSummary,
    ) -> String {
        if let Some(status) = summary.service_membership_status.as_deref() {
            return match status {
                "ok" => "ok",
                "acl_restricted" => "acl_restricted",
                "association_budget_exhausted" => "association_budget_exhausted",
                "page_budget_exhausted" => "page_budget_exhausted",
                "not_attempted" => "not_attempted",
                _ => "not_attempted",
            }
            .to_string();
        }
        if summary.service_membership_association_limit_reached {
            "association_budget_exhausted".to_string()
        } else if summary.service_membership_page_limit_reached {
            "page_budget_exhausted".to_string()
        } else {
            "not_attempted".to_string()
        }
    }

    pub(super) fn business_application_inventory_health_status(
        relationship_status: &str,
        service_membership_status: &str,
        summary: &BusinessApplicationServersSummary,
    ) -> &'static str {
        if summary.truncated {
            "truncated"
        } else if relationship_status != "ok" {
            "relationship_degraded"
        } else if !matches!(service_membership_status, "ok" | "not_attempted") {
            "service_membership_degraded"
        } else {
            "complete"
        }
    }

    /// Resolve the relationship-type allowlist used to filter `cmdb_rel_ci`
    /// edges during traversal.
    ///
    /// Relationship-type matching keys off the *stable* `cmdb_rel_type` identity
    /// (sys_id), not the mutable/localizable display label. The behavior:
    ///
    /// - Explicit non-empty allowlist: returned verbatim. Callers may pass
    ///   sys_ids or labels; [`BusinessApplicationRelationshipType::matches_any`]
    ///   compares against both the edge raw value and display label.
    /// - Empty allowlist with `defaults_when_empty == false`: returns empty,
    ///   which `matches_any` treats as "match all".
    /// - Empty allowlist with `defaults_when_empty == true`: resolves the default
    ///   label set ([`BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES`])
    ///   to `cmdb_rel_type` sys_ids via a single `name IN (...)` query, so each
    ///   edge is matched by sys_id identity. If resolution fails or returns
    ///   nothing (e.g. ACL-restricted `cmdb_rel_type`), it falls back to the
    ///   default label strings so behavior is no worse than the prior label-only
    ///   matching.
    pub(super) async fn resolve_relationship_type_allowlist(
        &self,
        options: &BusinessApplicationServersOptions,
        defaults_when_empty: bool,
    ) -> Result<Vec<String>> {
        if !options.relationship_type.is_empty() {
            // Explicit caller-supplied allowlist: use as-is.
            return Ok(options.relationship_type.clone());
        }
        if !defaults_when_empty {
            // Explicit empty allowlist => match all.
            return Ok(Vec::new());
        }

        let default_labels = BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES
            .iter()
            .map(|label| (*label).to_string())
            .collect::<Vec<_>>();
        let label_refs = default_labels
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        // One lookup per traversal: resolve the stored `name` of each default
        // relationship type to its sys_id. `cmdb_rel_type.name` is the configured
        // stored name (stable), whereas the edge display value can be localized.
        let response = self
            .ctx
            .client
            .table(CMDB_REL_TYPE_TABLE)
            .in_list("name", &label_refs)
            .fields(&["sys_id", "name"])
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(label_refs.len() as u32)
            .execute()
            .await;

        match response {
            Ok(response) => {
                let mut allowed = Vec::new();
                for record in response.records {
                    // `cmdb_rel_type.sys_id` is the stable relationship-type
                    // identity. Read the canonical record sys_id directly.
                    if let Ok(sys_id) = normalize_record_lookup_sys_id(&record.sys_id)
                        && !allowed.contains(&sys_id)
                    {
                        allowed.push(sys_id);
                    }
                }
                if allowed.is_empty() {
                    // Resolution returned nothing useful; fall back to label
                    // matching so the traversal is not silently emptied.
                    Ok(default_labels)
                } else {
                    Ok(allowed)
                }
            }
            // Degrade gracefully: if cmdb_rel_type cannot be read, fall back to
            // matching by the default label strings (prior behavior).
            Err(_) => Ok(default_labels),
        }
    }

    pub(super) async fn resolve_business_application_servers_selector(
        &self,
        options: &BusinessApplicationServersOptions,
    ) -> Result<Option<BusinessApplication>> {
        let aliases = self.resolve_business_application_aliases(false).await;
        let record = match &options.selector {
            BusinessApplicationServersSelector::SysId(sys_id) => match self
                .ctx
                .client
                .table(BUSINESS_APPLICATION_TABLE)
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(sys_id)
                .await
            {
                Ok(record) => Some(record),
                Err(SnowApiError::Api { status: 404, .. }) => None,
                Err(err) => return Err(err.into()),
            },
            BusinessApplicationServersSelector::Number(number) => {
                let records = self
                    .ctx
                    .client
                    .table(BUSINESS_APPLICATION_TABLE)
                    .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
                    .equals("number", number)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .limit(2)
                    .execute()
                    .await?
                    .records;
                if records.len() > 1 {
                    anyhow::bail!("multiple Business Applications matched number={number}");
                }
                records.into_iter().next()
            }
        };

        let Some(record) = record else {
            return Ok(None);
        };
        Ok(Some(BusinessApplication::from_servicenow(
            &record, &aliases,
        )?))
    }

    pub(super) async fn business_application_relationship_level(
        &self,
        frontier: &[String],
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<Vec<BusinessApplicationRelationshipEdge>> {
        // The remaining `max_edges` budget for THIS level is fixed up front from
        // the current running total. Both directions (parent + child) are
        // independent remote reads, so we issue them concurrently. Each reader is
        // bounded by the full `remaining` budget (so neither direction alone can
        // exceed the level budget), then we MERGE deterministically below in a
        // fixed parent-then-child order. The merge — not the concurrent reads —
        // owns the shared `summary`/`max_edges` accounting, so the result is
        // identical to the previous sequential version and reproducible.
        if frontier.is_empty() || summary.edge_limit_reached {
            return Ok(Vec::new());
        }
        let remaining = options
            .max_edges
            .saturating_sub(summary.relationships_examined);
        if remaining == 0 {
            mark_business_application_edge_limit(summary, diagnostics);
            return Ok(Vec::new());
        }

        let (parent_read, child_read) = tokio::try_join!(
            self.business_application_relationship_direction_read("parent", frontier, remaining),
            self.business_application_relationship_direction_read("child", frontier, remaining),
        )?;

        // Merge in a stable order: parent first, then child. Parent consumes the
        // shared budget first (exactly as the old sequential code did), and the
        // child's contribution is then capped to whatever budget the parent left.
        let mut records = Vec::new();
        let mut budget = remaining;
        for read in [parent_read, child_read] {
            budget = self.merge_business_application_direction_read(
                read,
                budget,
                summary,
                diagnostics,
                &mut records,
            );
        }

        let mut edges = Vec::new();
        let mut seen = HashSet::new();
        for record in records {
            let Some(edge) =
                business_application_relationship_edge_from_record(&record, summary, diagnostics)
            else {
                continue;
            };
            if seen.insert(edge.key()) {
                edges.push(edge);
            }
        }
        Ok(edges)
    }

    /// Folds one direction's read result into the shared traversal accounting and
    /// returns the budget remaining for the next direction.
    ///
    /// This is the single place that mutates `summary`/`diagnostics` for the edge
    /// read, so the two concurrent direction reads stay side-effect-free and the
    /// budget is consumed in a deterministic order. The direction's collected rows
    /// are truncated to `budget`; if that truncation drops rows, or the read
    /// itself was truncated by its own bound while pages remained, the shared
    /// `edge_limit_reached` flag is set. ACL failures are surfaced exactly once
    /// per direction.
    pub(super) fn merge_business_application_direction_read(
        &self,
        read: BusinessApplicationDirectionRead,
        budget: usize,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
        records: &mut Vec<Record>,
    ) -> usize {
        if read.acl_restricted {
            summary.acl_restricted_count += 1;
            summary.relationship_acl_restricted_count += 1;
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                read.field,
                CMDB_REL_CI_TABLE,
                "",
                ReferenceResolutionReason::ReferenceAclRestricted,
                "ACL restricted cmdb_rel_ci traversal",
            );
        }

        let mut rows = read.records;
        // Cap this direction's rows at the budget left after the prior direction.
        let over_budget = rows.len() > budget;
        if over_budget {
            rows.truncate(budget);
            mark_business_application_edge_limit(summary, diagnostics);
        } else if read.edge_limit_reached {
            // The direction read consumed its own bound while pages remained; this
            // is a genuine truncation that survives the merge.
            mark_business_application_edge_limit(summary, diagnostics);
        }

        let consumed = rows.len();
        summary.relationships_examined += consumed;
        records.extend(rows);
        budget.saturating_sub(consumed)
    }

    /// Reads one relationship direction (`parent` or `child`) of the `cmdb_rel_ci`
    /// edges for the current frontier, bounded by `remaining` edges.
    ///
    /// This is a PURE read: it issues remote requests and returns the collected
    /// rows plus truncation/ACL flags, but mutates no shared traversal state. That
    /// lets the two directions run concurrently; all `summary`/`diagnostics`
    /// accounting is applied afterwards by
    /// [`Self::merge_business_application_direction_read`] in a deterministic
    /// order. The read stops once `remaining` rows are collected (flagging
    /// `edge_limit_reached` if pages still remain) or the result set is exhausted.
    pub(super) async fn business_application_relationship_direction_read(
        &self,
        field: &'static str,
        frontier: &[String],
        remaining: usize,
    ) -> Result<BusinessApplicationDirectionRead> {
        let mut read = BusinessApplicationDirectionRead::new(field);
        if frontier.is_empty() || remaining == 0 {
            return Ok(read);
        }

        let frontier_refs = frontier.iter().map(String::as_str).collect::<Vec<_>>();
        // Paginate the edge read instead of issuing one large `limit(remaining+1)`
        // request. `servicenow_rs::execute()` does NOT auto-paginate, so a single
        // request can be silently capped server-side by
        // `glide.rest.table.max_record_count`, returning FEWER rows than exist
        // with no signal — edges would then vanish and `edge_limit_reached` would
        // stay false. The paginator walks pages until the result set is exhausted
        // or the `remaining` budget is consumed.
        let page_size = BUSINESS_APPLICATION_RELATIONSHIP_PAGE_SIZE.min(remaining.max(1));
        let mut paginator = self
            .ctx
            .client
            .table(CMDB_REL_CI_TABLE)
            .in_list(field, &frontier_refs)
            .fields(BUSINESS_APPLICATION_RELATIONSHIP_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(page_size as u32)
            .paginate()?;

        let mut collected = 0usize;
        loop {
            if collected >= remaining {
                // Budget exhausted but the paginator may still have pages: this is
                // a genuine truncation, surface it via the flag for the merge step.
                if !paginator.is_done() {
                    read.edge_limit_reached = true;
                }
                break;
            }
            let page = match paginator.next_page().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(err) if is_servicenow_acl_error(&err) => {
                    read.acl_restricted = true;
                    return Ok(read);
                }
                Err(err) => return Err(err.into()),
            };

            let mut rows = page.records;
            // Cap the running total at the remaining edge budget.
            let room = remaining - collected;
            let over_budget = rows.len() > room;
            if over_budget {
                rows.truncate(room);
                read.edge_limit_reached = true;
            }
            collected += rows.len();
            read.records.extend(rows);
            if over_budget {
                break;
            }
        }
        Ok(read)
    }

    pub(super) async fn business_application_service_membership_servers(
        &self,
        service_sys_ids: &HashSet<String>,
        service_paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
        server_class_cache: &mut HashMap<String, bool>,
    ) -> Result<Vec<(Server, Vec<BusinessApplicationServerPath>)>> {
        if service_sys_ids.is_empty() {
            summary.service_membership_status = Some("not_attempted".to_string());
            return Ok(Vec::new());
        }

        let read = self
            .business_application_service_membership_read(service_sys_ids, options)
            .await?;
        summary.service_membership_pages_examined += read.pages_examined;
        if read.acl_restricted {
            summary.acl_restricted_count += 1;
            summary.service_membership_status = Some("acl_restricted".to_string());
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "svc_ci_assoc",
                SVC_CI_ASSOC_TABLE,
                "",
                ReferenceResolutionReason::ReferenceAclRestricted,
                "ACL restricted svc_ci_assoc service membership read",
            );
            return Ok(Vec::new());
        }
        if read.association_limit_reached {
            summary.service_membership_association_limit_reached = true;
            summary.mark_truncated(1);
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "svc_ci_assoc",
                SVC_CI_ASSOC_TABLE,
                "",
                ReferenceResolutionReason::FanoutLimitExceeded,
                "max_service_membership_associations prevented reading more associations",
            );
        }
        if read.page_limit_reached {
            summary.service_membership_page_limit_reached = true;
            summary.mark_truncated(1);
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "svc_ci_assoc",
                SVC_CI_ASSOC_TABLE,
                "",
                ReferenceResolutionReason::FanoutLimitExceeded,
                "max_service_membership_pages prevented reading more association pages",
            );
        }
        summary.service_membership_associations_examined += read.records.len();

        let mut server_ids = Vec::new();
        let mut server_paths_by_sys_id: BTreeMap<String, Vec<BusinessApplicationServerPath>> =
            BTreeMap::new();
        let mut examined_member_cis = HashSet::new();
        for record in read.records {
            let Some(service_sys_id) = servicenow_reference_sys_id(&record, "service_id") else {
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "service_id",
                    SVC_CI_ASSOC_TABLE,
                    "",
                    ReferenceResolutionReason::ReferenceResolutionFailed,
                    "svc_ci_assoc row was missing a readable service_id reference",
                );
                continue;
            };
            let Some(ci_sys_id) = servicenow_reference_sys_id(&record, "ci_id") else {
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "ci_id",
                    SVC_CI_ASSOC_TABLE,
                    "",
                    ReferenceResolutionReason::ReferenceResolutionFailed,
                    "svc_ci_assoc row was missing a readable ci_id reference",
                );
                continue;
            };
            if examined_member_cis.insert(ci_sys_id.clone()) {
                if summary.cis_examined.saturating_sub(1) >= options.max_cis {
                    summary.ci_limit_reached = true;
                    summary.mark_truncated(1);
                    push_business_application_server_diagnostic(
                        summary,
                        diagnostics,
                        "ci_id",
                        SVC_CI_ASSOC_TABLE,
                        &ci_sys_id,
                        ReferenceResolutionReason::FanoutLimitExceeded,
                        "max_cis prevented examining another service member CI",
                    );
                    continue;
                }
                summary.cis_examined += 1;
            }

            let Some(class_name) = servicenow_record_text(&record, "ci_id.sys_class_name") else {
                summary.missing_ci_count += 1;
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "ci_id.sys_class_name",
                    SVC_CI_ASSOC_TABLE,
                    &ci_sys_id,
                    ReferenceResolutionReason::ReferenceResolutionFailed,
                    "svc_ci_assoc member CI class could not be read",
                );
                continue;
            };
            let is_server = self
                .business_application_class_is_server(
                    &class_name,
                    server_class_cache,
                    summary,
                    diagnostics,
                )
                .await?;
            if !is_server {
                continue;
            }
            if !server_ids.contains(&ci_sys_id) {
                server_ids.push(ci_sys_id.clone());
            }
            server_paths_by_sys_id
                .entry(ci_sys_id.clone())
                .or_default()
                .extend(business_application_service_membership_paths_for(
                    service_paths_by_ci,
                    &service_sys_id,
                    &ci_sys_id,
                ));
        }

        let servers = self
            .business_application_hydrate_servers(&server_ids, summary, diagnostics)
            .await?;
        if summary.service_membership_status.is_none() {
            if summary.service_membership_association_limit_reached {
                summary.service_membership_status =
                    Some("association_budget_exhausted".to_string());
            } else if summary.service_membership_page_limit_reached {
                summary.service_membership_status = Some("page_budget_exhausted".to_string());
            } else {
                summary.service_membership_status = Some("ok".to_string());
            }
        }

        Ok(servers
            .into_iter()
            .map(|server| {
                let paths = server_paths_by_sys_id
                    .remove(&server.record.sys_id)
                    .unwrap_or_default();
                (server, paths)
            })
            .collect())
    }

    pub(super) async fn business_application_service_membership_read(
        &self,
        service_sys_ids: &HashSet<String>,
        options: &BusinessApplicationServersOptions,
    ) -> Result<BusinessApplicationServiceMembershipRead> {
        let mut read = BusinessApplicationServiceMembershipRead::new();
        if service_sys_ids.is_empty() {
            return Ok(read);
        }

        let mut service_refs = service_sys_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        service_refs.sort_unstable();
        let page_size = BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_PAGE_SIZE
            .min(options.max_service_membership_associations.max(1));
        let mut paginator = self
            .ctx
            .client
            .table(SVC_CI_ASSOC_TABLE)
            .in_list("service_id", &service_refs)
            .fields(BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(page_size as u32)
            .paginate()?;

        let mut collected = 0usize;
        loop {
            if collected >= options.max_service_membership_associations {
                if !paginator.is_done() {
                    read.association_limit_reached = true;
                }
                break;
            }
            if read.pages_examined >= options.max_service_membership_pages {
                if !paginator.is_done() {
                    read.page_limit_reached = true;
                }
                break;
            }
            let page = match paginator.next_page().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(err) if is_servicenow_acl_error(&err) => {
                    read.acl_restricted = true;
                    return Ok(read);
                }
                Err(err) => return Err(err.into()),
            };
            read.pages_examined += 1;
            let mut rows = page.records;
            let room = options
                .max_service_membership_associations
                .saturating_sub(collected);
            let over_budget = rows.len() > room;
            if over_budget {
                rows.truncate(room);
                read.association_limit_reached = true;
            }
            collected += rows.len();
            read.records.extend(rows);
            if over_budget {
                break;
            }
        }
        Ok(read)
    }

    /// Decide whether a CMDB `sys_class_name` is a server, hierarchy-aware.
    ///
    /// Detection is layered cheapest-first:
    /// 1. The sync [`is_server_class`] heuristic (base table, known OOTB
    ///    subclasses, and the `cmdb_ci_*_server` naming convention) — no network.
    /// 2. For classes that pass none of those (instance-custom server subclasses
    ///    whose table name does NOT end in `_server`), a metadata-backed descent:
    ///    walk the `sys_db_object` `super_class` chain via [`Self::table_ancestors`]
    ///    and treat the class as a server iff `cmdb_ci_server` is an ancestor.
    ///
    /// The hierarchy result is memoized per `sys_class_name` in `server_class_cache`
    /// so the same unrecognized class is queried at most once per traversal.
    ///
    /// Degradation: if the metadata walk fails (e.g. ACL/network), the failure is
    /// surfaced as a `DictionaryUnavailable` diagnostic (never silent) and the
    /// class is treated as a NON-server. That is the non-destructive choice — the
    /// CI continues into the BFS frontier so its subtree is still explored, rather
    /// than being pruned as a leaf server. A class only reaches this fallback after
    /// failing every server-naming heuristic, so it is unlikely to be a server; and
    /// the negative cache entry stops repeated failing queries within the traversal.
    pub(super) async fn business_application_class_is_server(
        &self,
        class_name: &str,
        server_class_cache: &mut HashMap<String, bool>,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<bool> {
        // Cheap, network-free checks first.
        if is_server_class(class_name) {
            return Ok(true);
        }

        if let Some(cached) = server_class_cache.get(class_name) {
            return Ok(*cached);
        }

        // Metadata-backed super_class descent for unrecognized classes.
        let is_server = match self.ctx.table_ancestors(class_name).await {
            Ok(ancestors) => ancestors.iter().any(|ancestor| ancestor == SERVER_TABLE),
            Err(_) => {
                // Degrade without failing the traversal: surface the metadata gap
                // and fall back to NON-server (continue traversing through the CI).
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "sys_class_name",
                    "sys_db_object",
                    "",
                    ReferenceResolutionReason::DictionaryUnavailable,
                    "sys_db_object super_class lookup failed; class not classified as server",
                );
                false
            }
        };
        server_class_cache.insert(class_name.to_string(), is_server);
        Ok(is_server)
    }

    pub(super) async fn business_application_hydrate_ci_classes(
        &self,
        sys_ids: &[String],
        ci_classes: &mut HashMap<String, String>,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<()> {
        let pending = sys_ids
            .iter()
            .filter(|sys_id| !ci_classes.contains_key(sys_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let pending_refs = pending.iter().map(String::as_str).collect::<Vec<_>>();
        let response = self
            .ctx
            .client
            .table(CMDB_CI_TABLE)
            .in_list("sys_id", &pending_refs)
            .fields(BUSINESS_APPLICATION_CI_CLASS_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(pending.len() as u32)
            .execute()
            .await;

        let records = match response {
            Ok(response) => response.records,
            Err(SnowApiError::Api { status, .. }) if status == 401 || status == 403 => {
                summary.acl_restricted_count += pending.len();
                for sys_id in pending {
                    push_business_application_server_diagnostic(
                        summary,
                        diagnostics,
                        "sys_class_name",
                        CMDB_CI_TABLE,
                        &sys_id,
                        ReferenceResolutionReason::ReferenceAclRestricted,
                        "ACL restricted cmdb_ci class read",
                    );
                }
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };

        let mut found = HashSet::new();
        for record in records {
            let Some(sys_id) = servicenow_reference_sys_id(&record, "sys_id") else {
                continue;
            };
            if let Some(class_name) = servicenow_record_text(&record, "sys_class_name") {
                ci_classes.insert(sys_id.clone(), class_name);
                found.insert(sys_id);
            }
        }
        for sys_id in pending {
            if found.contains(&sys_id) {
                continue;
            }
            summary.missing_ci_count += 1;
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "sys_class_name",
                CMDB_CI_TABLE,
                &sys_id,
                ReferenceResolutionReason::ReferenceNotFound,
                "CI class could not be read",
            );
        }
        Ok(())
    }

    pub(super) async fn business_application_hydrate_servers(
        &self,
        sys_ids: &[String],
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<Vec<Server>> {
        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for sys_id in sys_ids {
            if seen.insert(sys_id) {
                unique.push(sys_id.clone());
            }
        }
        if unique.is_empty() {
            return Ok(Vec::new());
        }

        let sys_id_refs = unique.iter().map(String::as_str).collect::<Vec<_>>();
        // Query the BASE `cmdb_ci_server` table by `sys_id` only. In
        // ServiceNow's table-per-hierarchy model the base table transparently
        // returns rows of every subclass (linux, win, esx, aix, ...), so a
        // record exists here iff the CI is a true server. The previous
        // `sys_class_name IN SERVER_TABLES` filter was over-restrictive: it
        // excluded legitimate subclass servers whose class is not in the narrow
        // alias list, silently dropping them. Relying on the base-table query is
        // both correct (non-servers simply do not exist in `cmdb_ci_server`) and
        // hierarchy-complete.
        let response = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .in_list("sys_id", &sys_id_refs)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(unique.len() as u32)
            .execute()
            .await;

        let records = match response {
            Ok(response) => response.records,
            Err(SnowApiError::Api { status, .. }) if status == 401 || status == 403 => {
                summary.acl_restricted_count += unique.len();
                for sys_id in unique {
                    push_business_application_server_diagnostic(
                        summary,
                        diagnostics,
                        "sys_id",
                        SERVER_TABLE,
                        &sys_id,
                        ReferenceResolutionReason::ReferenceAclRestricted,
                        "ACL restricted server CI read",
                    );
                }
                return Ok(Vec::new());
            }
            Err(err) => return Err(err.into()),
        };

        let mut found = HashSet::new();
        let mut servers = Vec::new();
        for record in records {
            found.insert(record.sys_id.clone());
            servers.push(Server::from_servicenow(&record)?);
        }
        for sys_id in unique {
            if found.contains(&sys_id) {
                continue;
            }
            summary.missing_ci_count += 1;
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "sys_id",
                SERVER_TABLE,
                &sys_id,
                ReferenceResolutionReason::ReferenceNotFound,
                "server CI could not be read",
            );
        }
        Ok(servers)
    }
}
