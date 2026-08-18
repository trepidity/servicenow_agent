use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BusinessApplicationRelationshipEdge {
    pub(super) parent_sys_id: String,
    pub(super) child_sys_id: String,
    pub(super) parent_class: Option<String>,
    pub(super) child_class: Option<String>,
    pub(super) relationship_type: BusinessApplicationRelationshipType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct BusinessApplicationSearchParams {
    /// Substring match against cmdb_ci_business_app.name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Business Owner display name or sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub business_owner: Option<String>,
    /// IS Owner / IT Application Owner display name or sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_owner: Option<String>,
    /// CI owner group display name or sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<String>,
    /// Primary Support Group display name or sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_support_group: Option<String>,
    /// Operational state/status label or raw choice value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_state: Option<String>,
    /// Operational state/status label or raw choice value to exclude.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_state_not: Option<String>,
    /// Primary Portfolio display name or sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_portfolio: Option<String>,
    /// Exact attested date, YYYY-MM-DD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attested_date: Option<String>,
    /// Lower attested date bound, YYYY-MM-DD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attested_date_on_or_after: Option<String>,
    /// Upper attested date bound, YYYY-MM-DD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attested_date_on_or_before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl BusinessApplicationSearchParams {
    pub fn validate(&self) -> Result<()> {
        self.validated_limit()?;
        for (name, value) in [
            ("attested_date", self.attested_date.as_deref()),
            (
                "attested_date_on_or_after",
                self.attested_date_on_or_after.as_deref(),
            ),
            (
                "attested_date_on_or_before",
                self.attested_date_on_or_before.as_deref(),
            ),
        ] {
            if let Some(value) = non_empty_owned(value)
                && parse_servicenow_date(Some(&value)).is_none()
            {
                anyhow::bail!("`{name}` must be YYYY-MM-DD");
            }
        }
        Ok(())
    }

    pub(super) fn validated_limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(BUSINESS_APPLICATION_DEFAULT_LIMIT);
        if limit == 0 {
            anyhow::bail!("`limit` must be at least 1");
        }
        if limit > BUSINESS_APPLICATION_MAX_LIMIT {
            anyhow::bail!("`limit` must be at most {BUSINESS_APPLICATION_MAX_LIMIT}");
        }
        Ok(limit)
    }
}

pub(super) fn normalize_operational_state(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase().replace(['_', '-'], " ");
    match normalized.as_str() {
        "operational" => "1".to_string(),
        "non operational" | "nonoperational" | "not operational" => "2".to_string(),
        "repair in progress" => "3".to_string(),
        "dr standby" | "disaster recovery standby" => "4".to_string(),
        "ready" => "5".to_string(),
        "retired" => "6".to_string(),
        _ => value.trim().to_string(),
    }
}

pub(super) fn roll_up_business_application_summary(
    summary: &mut BusinessApplicationSyncSummary,
    applications: &[BusinessApplication],
) {
    for application in applications {
        // Resolved references are tracked on descriptors; unresolved references
        // surface as diagnostics. Count each independently so totals stay
        // meaningful even when a reference appears in both lists.
        summary.references_resolved += application
            .references
            .iter()
            .filter(|descriptor| {
                descriptor.resolution_status == ReferenceResolutionStatus::Resolved
            })
            .count();
        for diagnostic in &application.unresolved_references {
            summary.references_unresolved += 1;
            *summary
                .degraded_reasons
                .entry(diagnostic.reason.as_key().to_string())
                .or_insert(0) += 1;
            if diagnostic.reason.is_dictionary_unavailable() {
                summary.dictionary_degraded = true;
            }
        }
    }
}

/// Result of a single side-effect-free relationship-direction read.
///
/// Carries the collected `cmdb_rel_ci` rows plus the flags the merge step needs
/// to update the shared traversal accounting. Keeping this separate from
/// `summary`/`diagnostics` is what lets the two directions run concurrently:
/// neither read touches shared state, and the merge applies side effects in a
/// deterministic order.
pub(super) struct BusinessApplicationDirectionRead {
    /// The `parent`/`child` field this read traversed (used for diagnostics).
    pub(super) field: &'static str,
    /// Collected edge rows, already bounded to the per-read `remaining` budget.
    pub(super) records: Vec<Record>,
    /// Set when the read consumed its budget while more pages remained.
    pub(super) edge_limit_reached: bool,
    /// Set when a 401/403 ACL error stopped the read.
    pub(super) acl_restricted: bool,
}

#[derive(Debug, Clone)]
pub(super) struct BusinessApplicationServiceMembershipRead {
    pub(super) records: Vec<Record>,
    pub(super) pages_examined: usize,
    pub(super) association_limit_reached: bool,
    pub(super) page_limit_reached: bool,
    pub(super) acl_restricted: bool,
}

impl BusinessApplicationServiceMembershipRead {
    pub(super) fn new() -> Self {
        Self {
            records: Vec::new(),
            pages_examined: 0,
            association_limit_reached: false,
            page_limit_reached: false,
            acl_restricted: false,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct BusinessApplicationServerDiscovery {
    pub(super) server: Server,
    pub(super) provenance: BusinessApplicationServerProvenance,
    pub(super) relationship_paths: Vec<BusinessApplicationServerPath>,
    pub(super) service_membership_paths: Vec<BusinessApplicationServerPath>,
}

impl BusinessApplicationServerDiscovery {
    pub(super) fn new(
        server: Server,
        provenance: BusinessApplicationServerProvenance,
        paths: Vec<BusinessApplicationServerPath>,
    ) -> Self {
        let mut discovery = Self {
            server,
            provenance,
            relationship_paths: Vec::new(),
            service_membership_paths: Vec::new(),
        };
        discovery.add_paths(provenance, paths);
        discovery
    }

    pub(super) fn add_paths(
        &mut self,
        provenance: BusinessApplicationServerProvenance,
        paths: Vec<BusinessApplicationServerPath>,
    ) {
        self.provenance = self.provenance.merge(provenance);
        match provenance {
            BusinessApplicationServerProvenance::Relationship => {
                self.relationship_paths.extend(paths);
            }
            BusinessApplicationServerProvenance::ServiceMembership => {
                self.service_membership_paths.extend(paths);
            }
            BusinessApplicationServerProvenance::Both => {
                self.relationship_paths.extend(paths.clone());
                self.service_membership_paths.extend(paths);
            }
        }
    }

    pub(super) fn paths(&self) -> Vec<BusinessApplicationServerPath> {
        let mut paths = self.relationship_paths.clone();
        paths.extend(self.service_membership_paths.clone());
        paths
    }
}

impl BusinessApplicationDirectionRead {
    pub(super) fn new(field: &'static str) -> Self {
        Self {
            field,
            records: Vec::new(),
            edge_limit_reached: false,
            acl_restricted: false,
        }
    }
}

impl BusinessApplicationRelationshipEdge {
    pub(super) fn key(&self) -> (String, String, String, Option<String>) {
        (
            self.parent_sys_id.clone(),
            self.child_sys_id.clone(),
            self.relationship_type.value.clone(),
            self.relationship_type.display_value.clone(),
        )
    }

    pub(super) fn traversal_endpoint(
        &self,
        frontier: &HashSet<String>,
    ) -> Option<(String, String, BusinessApplicationRelationshipDirection)> {
        if frontier.contains(&self.parent_sys_id) {
            Some((
                self.parent_sys_id.clone(),
                self.child_sys_id.clone(),
                BusinessApplicationRelationshipDirection::ParentToChild,
            ))
        } else if frontier.contains(&self.child_sys_id) {
            Some((
                self.child_sys_id.clone(),
                self.parent_sys_id.clone(),
                BusinessApplicationRelationshipDirection::ChildToParent,
            ))
        } else {
            None
        }
    }

    /// Build a path edge for the route bookkeeping. The traversal endpoints
    /// (`from`/`to`) are NOT stored: they are derived on demand from
    /// `parent_sys_id`/`child_sys_id`/`direction` via
    /// [`BusinessApplicationServerPathEdge::from_sys_id`] /
    /// [`BusinessApplicationServerPathEdge::to_sys_id`], so the caller need only
    /// supply the BFS `direction` it crossed the edge in.
    pub(super) fn path_edge(
        &self,
        depth: usize,
        direction: BusinessApplicationRelationshipDirection,
    ) -> BusinessApplicationServerPathEdge {
        BusinessApplicationServerPathEdge {
            depth,
            parent_sys_id: self.parent_sys_id.clone(),
            child_sys_id: self.child_sys_id.clone(),
            direction,
            relationship_type: self.relationship_type.clone(),
            edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
        }
    }
}

pub(super) fn business_application_relationship_edge_from_record(
    record: &Record,
    summary: &mut BusinessApplicationServersSummary,
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
) -> Option<BusinessApplicationRelationshipEdge> {
    let parent_sys_id = servicenow_reference_sys_id(record, "parent");
    let child_sys_id = servicenow_reference_sys_id(record, "child");
    let (Some(parent_sys_id), Some(child_sys_id)) = (parent_sys_id, child_sys_id) else {
        push_business_application_server_diagnostic(
            summary,
            diagnostics,
            "cmdb_rel_ci",
            CMDB_REL_CI_TABLE,
            "",
            ReferenceResolutionReason::ReferenceResolutionFailed,
            "relationship row was missing a readable parent or child reference",
        );
        return None;
    };

    Some(BusinessApplicationRelationshipEdge {
        parent_sys_id,
        child_sys_id,
        parent_class: servicenow_record_text(record, "parent.sys_class_name"),
        child_class: servicenow_record_text(record, "child.sys_class_name"),
        relationship_type: BusinessApplicationRelationshipType {
            value: servicenow_record_raw_text(record, "type").unwrap_or_default(),
            display_value: servicenow_record_display_text(record, "type"),
        },
    })
}

/// All CI sys_ids that lie along a recorded route chain, i.e. every CI the
/// route passes through. A chain is a list of edges; the CIs it touches are the
/// `from_sys_id` of the first edge plus the `to_sys_id` of each edge.
pub(super) fn chain_node_sys_ids(chain: &[BusinessApplicationServerPathEdge]) -> HashSet<String> {
    let mut nodes = HashSet::new();
    if let Some(first) = chain.first() {
        nodes.insert(first.from_sys_id().to_string());
    }
    for edge in chain {
        nodes.insert(edge.to_sys_id().to_string());
    }
    nodes
}

/// Decide whether reaching `adjacent_sys_id` from `current_sys_id` is a true
/// back-edge (cycle) rather than an alternate forward path.
///
/// It is a back-edge when `adjacent_sys_id` is an ancestor of
/// `current_sys_id` — that is, the adjacent CI already lies on some route that
/// leads to the current CI, so following this edge would loop back up the graph.
/// If the adjacent CI does not appear on any of the current CI's chains, the
/// edge is a genuine alternate route to an already-visited node (a diamond
/// join), not a cycle.
///
/// When the current CI has no recorded chains (paths disabled or unknown), we
/// conservatively treat the re-visit as a back-edge to preserve the prior
/// cycle-counting behavior.
pub(super) fn path_chains_contain_ancestor(
    current_chains: Option<&Vec<Vec<BusinessApplicationServerPathEdge>>>,
    adjacent_sys_id: &str,
    current_sys_id: &str,
) -> bool {
    let Some(chains) = current_chains else {
        return true;
    };
    if chains.is_empty() {
        return true;
    }
    // The current CI itself is trivially "on" its own path; reaching it again is
    // a self-loop / back-edge.
    if adjacent_sys_id == current_sys_id {
        return true;
    }
    chains
        .iter()
        .any(|chain| chain_node_sys_ids(chain).contains(adjacent_sys_id))
}

pub(super) fn business_application_server_paths_for(
    paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    sys_id: &str,
) -> Vec<BusinessApplicationServerPath> {
    paths_by_ci
        .get(sys_id)
        .map(|chains| {
            chains
                .iter()
                .map(|chain| BusinessApplicationServerPath {
                    edges: chain.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn business_application_service_membership_paths_for(
    service_paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    service_sys_id: &str,
    server_sys_id: &str,
) -> Vec<BusinessApplicationServerPath> {
    let base_chains = service_paths_by_ci
        .get(service_sys_id)
        .cloned()
        .unwrap_or_default();
    if base_chains.is_empty() {
        return vec![BusinessApplicationServerPath {
            edges: vec![BusinessApplicationServerPathEdge {
                depth: 1,
                parent_sys_id: service_sys_id.to_string(),
                child_sys_id: server_sys_id.to_string(),
                direction: BusinessApplicationRelationshipDirection::ParentToChild,
                relationship_type: BusinessApplicationRelationshipType {
                    value: "service_member_of".to_string(),
                    display_value: Some("service_member_of".to_string()),
                },
                edge_source: BusinessApplicationServerPathEdgeSource::ServiceMembership,
            }],
        }];
    }

    base_chains
        .into_iter()
        .map(|mut chain| {
            let depth = chain.len() + 1;
            chain.push(BusinessApplicationServerPathEdge {
                depth,
                parent_sys_id: service_sys_id.to_string(),
                child_sys_id: server_sys_id.to_string(),
                direction: BusinessApplicationRelationshipDirection::ParentToChild,
                relationship_type: BusinessApplicationRelationshipType {
                    value: "service_member_of".to_string(),
                    display_value: Some("service_member_of".to_string()),
                },
                edge_source: BusinessApplicationServerPathEdgeSource::ServiceMembership,
            });
            BusinessApplicationServerPath { edges: chain }
        })
        .collect()
}

pub(super) fn merge_business_application_server_discovery(
    discoveries: &mut BTreeMap<String, BusinessApplicationServerDiscovery>,
    server: Server,
    provenance: BusinessApplicationServerProvenance,
    paths: Vec<BusinessApplicationServerPath>,
) {
    let sys_id = server.record.sys_id.clone();
    discoveries
        .entry(sys_id)
        .and_modify(|entry| entry.add_paths(provenance, paths.clone()))
        .or_insert_with(|| BusinessApplicationServerDiscovery::new(server, provenance, paths));
}

/// Extend every recorded route chain of `parent_sys_id` by `edge` and append the
/// resulting chains to `child_sys_id`'s recorded routes.
///
/// On first discovery this seeds the child's routes; on a diamond join it adds
/// the alternate route(s) without disturbing the existing ones. The child's
/// existing chains are preserved (so multiple distinct parents accumulate).
pub(super) fn extend_path_chains(
    paths_by_ci: &mut HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    parent_sys_id: &str,
    child_sys_id: &str,
    edge: BusinessApplicationServerPathEdge,
) -> bool {
    let new_chains = extended_path_chains(paths_by_ci, parent_sys_id, edge);
    if new_chains.is_empty() {
        return false;
    }
    paths_by_ci
        .entry(child_sys_id.to_string())
        .or_default()
        .extend(new_chains);
    true
}

pub(super) fn extended_path_chains(
    paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    parent_sys_id: &str,
    edge: BusinessApplicationServerPathEdge,
) -> Vec<Vec<BusinessApplicationServerPathEdge>> {
    let parent_chains = paths_by_ci.get(parent_sys_id).cloned().unwrap_or_default();
    if parent_chains.is_empty() {
        // Parent has no recorded chain (e.g. paths disabled): record a
        // single-edge chain so traversal bookkeeping still has one route.
        vec![vec![edge]]
    } else {
        parent_chains
            .into_iter()
            .filter_map(|mut chain| {
                if let Some(first) = chain.first()
                    && first.direction != edge.direction
                {
                    return None;
                }
                chain.push(edge.clone());
                Some(chain)
            })
            .collect()
    }
}

/// Emit any not-yet-recorded route chains for an already-collected server.
///
/// Used when a server is reached again via an alternate diamond route after it
/// was first hydrated: its newly-added chains must be reflected in
/// `server_paths`. Only chains beyond those already present are appended, so
/// repeated edges in a single level do not double-count.
pub(super) fn emit_alternate_server_path(
    servers_by_sys_id: &BTreeMap<String, Server>,
    paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    server_paths: &mut BTreeMap<String, Vec<BusinessApplicationServerPath>>,
    sys_id: &str,
) {
    if !servers_by_sys_id.contains_key(sys_id) {
        return;
    }
    let Some(chains) = paths_by_ci.get(sys_id) else {
        return;
    };
    let entry = server_paths.entry(sys_id.to_string()).or_default();
    // Re-sync the emitted routes with the recorded chains: append the chains
    // that have not yet been emitted (the new alternate routes are at the tail).
    while entry.len() < chains.len() {
        let chain = &chains[entry.len()];
        entry.push(BusinessApplicationServerPath {
            edges: chain.clone(),
        });
    }
}

pub(super) fn servicenow_record_display_text(record: &Record, field: &str) -> Option<String> {
    record
        .get_display(field)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn mark_business_application_edge_limit(
    summary: &mut BusinessApplicationServersSummary,
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
) {
    if summary.edge_limit_reached {
        return;
    }
    summary.edge_limit_reached = true;
    summary.mark_truncated(1);
    push_business_application_server_diagnostic(
        summary,
        diagnostics,
        "cmdb_rel_ci",
        CMDB_REL_CI_TABLE,
        "",
        ReferenceResolutionReason::FanoutLimitExceeded,
        "max_edges prevented reading more relationships",
    );
}

pub(super) fn push_business_application_server_diagnostic(
    summary: &mut BusinessApplicationServersSummary,
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    field: impl Into<String>,
    reference_table: impl Into<String>,
    reference_sys_id: impl Into<String>,
    reason: ReferenceResolutionReason,
    message: impl Into<String>,
) {
    summary.record_degraded_reason(&reason);
    diagnostics.push(ReferenceResolutionDiagnostic {
        field: field.into(),
        reference_table: reference_table.into(),
        reference_sys_id: reference_sys_id.into(),
        display_value: None,
        reason,
        message: Some(message.into()),
    });
}
