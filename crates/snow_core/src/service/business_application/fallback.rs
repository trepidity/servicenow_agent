use super::*;

impl BusinessApplicationService {
    /// `ci_owner_group` CMDB-gap fallback: when the `cmdb_rel_ci` traversal found
    /// 0 servers, query `cmdb_ci_server` by the BA's raw `u_ci_owner_group` field
    /// and return the matched servers, live-only (never persisted).
    ///
    /// Field mapping (the load-bearing correctness point): the group sys_id is
    /// sourced from the BA's raw `u_ci_owner_group` column via
    /// [`BusinessApplication::ci_owner_group_raw`] and filtered with an EXACT
    /// `u_ci_owner_group = <sys_id>` predicate on the server side. Neither side
    /// uses the `ci_owner_group`/`managed_by_group` typed alias, which is empty on
    /// live data.
    ///
    /// Bounds and degradation reuse the traversal contract:
    /// - the result set is bounded by `options.max_cis` and truncation is marked
    ///   via [`BusinessApplicationServersSummary::mark_truncated`];
    /// - HTTP 401/403 increments `acl_restricted_count` with a
    ///   `ReferenceAclRestricted` diagnostic and returns no servers
    ///   (`fallback_used = false`);
    /// - a BA with no `u_ci_owner_group`, or a tombstoned/unreadable group, emits a
    ///   structured diagnostic and returns `servers: []`, `fallback_used = false`
    ///   — never a panic or hard error;
    /// - returned rows are classified with the hierarchy-aware
    ///   [`is_server_class`], not `is_server_table`.
    pub(super) async fn business_application_ci_owner_group_fallback(
        &self,
        application: &BusinessApplication,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
        server_class_cache: &mut HashMap<String, bool>,
    ) -> Result<Vec<Server>> {
        // 1. Source the group from the RAW `u_ci_owner_group` field. Absent/empty
        //    is a clean diagnostic, not an error: fallback simply does not fire.
        let Some(group) = application.ci_owner_group_raw() else {
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                BUSINESS_APPLICATION_TABLE,
                &application.record.sys_id,
                ReferenceResolutionReason::ReferenceNotFound,
                "ci_owner_group fallback requested but BA has no u_ci_owner_group set",
            );
            return Ok(Vec::new());
        };

        // 2. EXACT filter on the raw `u_ci_owner_group` column. The fallback page
        //    size is the BA traversal's `max_cis` budget (default 500, ceiling
        //    5000) — NOT ServerSearchParams' SERVER_MAX_LIMIT (100). One extra row
        //    beyond the budget is requested so over-budget result sets can be
        //    detected and truncation marked deterministically.
        let limit = options.max_cis;
        let fetch_limit = limit.saturating_add(1).min(u32::MAX as usize) as u32;
        let response = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .equals(BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD, &group.sys_id)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .order_by("name", Order::Asc)
            .limit(fetch_limit)
            .execute()
            .await;

        let mut records = match response {
            Ok(response) => response.records,
            Err(SnowApiError::Api { status, .. }) if status == 401 || status == 403 => {
                summary.acl_restricted_count += 1;
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                    SERVER_TABLE,
                    &group.sys_id,
                    ReferenceResolutionReason::ReferenceAclRestricted,
                    "ACL restricted ci_owner_group fallback server query",
                );
                return Ok(Vec::new());
            }
            Err(err) if is_servicenow_acl_error(&err) => {
                summary.acl_restricted_count += 1;
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                    SERVER_TABLE,
                    &group.sys_id,
                    ReferenceResolutionReason::ReferenceAclRestricted,
                    "ACL restricted ci_owner_group fallback server query",
                );
                return Ok(Vec::new());
            }
            Err(SnowApiError::Api { status: 404, .. }) => {
                // Tombstoned/unreadable group: structured diagnostic, no servers,
                // no panic.
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                    SERVER_TABLE,
                    &group.sys_id,
                    ReferenceResolutionReason::ReferenceNotFound,
                    "ci_owner_group fallback group is tombstoned or unreadable",
                );
                return Ok(Vec::new());
            }
            Err(err) => return Err(err.into()),
        };

        // The fallback fired: record the group identity. `fallback_used` stays true
        // even when the group owns zero servers (the data-quality gap is real).
        summary.fallback_used = true;
        summary.fallback_group_sys_id = Some(group.sys_id.clone());
        summary.fallback_group_display_name = group.display_name.clone();
        summary.record_cmdb_relationships_unmapped();

        // Bound to `max_cis`; mark truncation for any rows beyond the budget.
        if records.len() > limit {
            let skipped = records.len() - limit;
            records.truncate(limit);
            summary.mark_truncated(skipped);
        }

        let mut servers = Vec::with_capacity(records.len());
        for record in records {
            // Hierarchy-aware class gate. The query targets the base
            // `cmdb_ci_server` table, so non-servers should not appear; this is a
            // defensive consistency check using `is_server_class` (not the narrow
            // `is_server_table`) so custom `cmdb_ci_*_server` subclasses are kept.
            let class_name = servicenow_record_text(&record, "sys_class_name");
            let is_server = match class_name.as_deref() {
                Some(class_name) => {
                    self.business_application_class_is_server(
                        class_name,
                        server_class_cache,
                        summary,
                        diagnostics,
                    )
                    .await?
                }
                // No class on a base-table row is unexpected but harmless: the row
                // came from cmdb_ci_server, so treat it as a server.
                None => true,
            };
            if !is_server {
                continue;
            }
            servers.push(Server::from_servicenow(&record)?);
        }
        Ok(servers)
    }
}
