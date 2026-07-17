//! `BusinessApplicationService` — Business Application search, live
//! relationship/service-membership graph traversal, dictionary-backed field
//! aliasing, and sync, extracted from the `SnowCore` god-object.
//!
//! Third domain service extracted in the library boundary migration
//! (Task 10), alongside `ServerService`, following the pattern `UserService`
//! (Task 8) and `ApprovalService` (Task 9) established. Every
//! method/helper/type/const body below is moved verbatim from its former
//! `impl SnowCore` / free-fn / module-level location in `lib.rs`.
//!
//! `BusinessApplicationSearchParams` also relocates here from `types.rs`
//! (Task 7 had landed it there as an interim step ahead of this move); its
//! `validate`/`validated_limit` inherent impl moves with it, unchanged.
//!
//! `apply_reference_name_or_sys_id_filter` and `is_servicenow_acl_error` are
//! shared with `ServerService` (Task 10's other extraction), so they were
//! relocated to `crate::helpers` rather than duplicated or privatized here.
//!
//! `BUSINESS_APPLICATION_TABLE` stays defined at the crate root (`lib.rs`):
//! the non-BA table-normalization helpers `canonical_record_table`,
//! `canonical_record_table_for_number`, and `normalize_record_lookup_table`
//! also depend on it, so it remains there and is reached here via `crate::`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use servicenow_rs::prelude::{DisplayValue, Error as SnowApiError, Operator, Order, Record};

use crate::cache::store::{
    BusinessApplicationFieldDictionaryRow, BusinessApplicationServerInventoryHealthRow,
    BusinessApplicationServerMembershipRow, PrimitiveObjectRow, PrimitiveResolutionStatus,
    ProjectedFieldRow,
};
use crate::context::CoreContext;
use crate::convert::serialize_record_document;
use crate::helpers::{
    apply_reference_name_or_sys_id_filter, is_servicenow_acl_error, non_empty_owned, parse_i64,
    servicenow_record_raw_text, servicenow_record_text, servicenow_reference_sys_id,
};
use crate::query::filter::BusinessApplicationQuery;
use crate::resource::business_application::{
    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES,
    BUSINESS_APPLICATION_SERVICE_DISCOVERY_RELATIONSHIP_TYPES, BusinessApplication,
    BusinessApplicationFieldAliases, BusinessApplicationHydrationOptions,
    BusinessApplicationLookup, BusinessApplicationRelationshipDirection,
    BusinessApplicationRelationshipType, BusinessApplicationServerApplication,
    BusinessApplicationServerInventoryHealth, BusinessApplicationServerPath,
    BusinessApplicationServerPathEdge, BusinessApplicationServerPathEdgeSource,
    BusinessApplicationServerProvenance, BusinessApplicationServersOptions,
    BusinessApplicationServersParams, BusinessApplicationServersResult,
    BusinessApplicationServersSelector, BusinessApplicationServersSummary,
    BusinessApplicationSyncSummary, FallbackStrategy, ReferencePrimitiveDescriptor,
    ReferencePrimitiveType, ReferenceResolutionDiagnostic, ReferenceResolutionReason,
    ReferenceResolutionStatus, ServerResultSource,
};
use crate::resource::server::{SERVER_TABLE, Server, is_server_class};
use crate::{
    BUSINESS_APPLICATION_TABLE, SnowRecord, normalize_record_lookup_sys_id, parse_servicenow_date,
    record_bool, record_field_display_or_raw, record_field_raw_or_display, vault,
};

const BUSINESS_APPLICATION_DEFAULT_LIMIT: usize = 20;

const BUSINESS_APPLICATION_MAX_LIMIT: usize = 100;

const BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE: usize = BUSINESS_APPLICATION_MAX_LIMIT;

const CMDB_CI_TABLE: &str = "cmdb_ci";

const CMDB_REL_CI_TABLE: &str = "cmdb_rel_ci";

const CMDB_REL_TYPE_TABLE: &str = "cmdb_rel_type";

const SVC_CI_ASSOC_TABLE: &str = "svc_ci_assoc";

const BUSINESS_APPLICATION_RELATIONSHIP_FIELDS: &[&str] = &[
    "sys_id",
    "parent",
    "child",
    "type",
    "parent.sys_class_name",
    "child.sys_class_name",
];
const BUSINESS_APPLICATION_CI_CLASS_FIELDS: &[&str] = &["sys_id", "name", "sys_class_name"];

const BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_FIELDS: &[&str] = &[
    "sys_id",
    "service_id",
    "service_id.sys_class_name",
    "ci_id",
    "ci_id.sys_class_name",
];
/// Page size for paginated `cmdb_rel_ci` edge reads. Kept well below the typical
/// `glide.rest.table.max_record_count` server cap (default 10000) so the read
/// never relies on a single oversized request that the instance could silently
/// truncate. The paginator continues across pages until the `max_edges` budget
/// is reached or the result set is exhausted.
const BUSINESS_APPLICATION_RELATIONSHIP_PAGE_SIZE: usize = 1000;

const BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_PAGE_SIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BusinessApplicationRelationshipEdge {
    parent_sys_id: String,
    child_sys_id: String,
    parent_class: Option<String>,
    child_class: Option<String>,
    relationship_type: BusinessApplicationRelationshipType,
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

    fn validated_limit(&self) -> Result<usize> {
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

fn normalize_operational_state(value: &str) -> String {
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

fn roll_up_business_application_summary(
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
struct BusinessApplicationDirectionRead {
    /// The `parent`/`child` field this read traversed (used for diagnostics).
    field: &'static str,
    /// Collected edge rows, already bounded to the per-read `remaining` budget.
    records: Vec<Record>,
    /// Set when the read consumed its budget while more pages remained.
    edge_limit_reached: bool,
    /// Set when a 401/403 ACL error stopped the read.
    acl_restricted: bool,
}

#[derive(Debug, Clone)]
struct BusinessApplicationServiceMembershipRead {
    records: Vec<Record>,
    pages_examined: usize,
    association_limit_reached: bool,
    page_limit_reached: bool,
    acl_restricted: bool,
}

impl BusinessApplicationServiceMembershipRead {
    fn new() -> Self {
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
struct BusinessApplicationServerDiscovery {
    server: Server,
    provenance: BusinessApplicationServerProvenance,
    relationship_paths: Vec<BusinessApplicationServerPath>,
    service_membership_paths: Vec<BusinessApplicationServerPath>,
}

impl BusinessApplicationServerDiscovery {
    fn new(
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

    fn add_paths(
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

    fn paths(&self) -> Vec<BusinessApplicationServerPath> {
        let mut paths = self.relationship_paths.clone();
        paths.extend(self.service_membership_paths.clone());
        paths
    }
}

impl BusinessApplicationDirectionRead {
    fn new(field: &'static str) -> Self {
        Self {
            field,
            records: Vec::new(),
            edge_limit_reached: false,
            acl_restricted: false,
        }
    }
}

impl BusinessApplicationRelationshipEdge {
    fn key(&self) -> (String, String, String, Option<String>) {
        (
            self.parent_sys_id.clone(),
            self.child_sys_id.clone(),
            self.relationship_type.value.clone(),
            self.relationship_type.display_value.clone(),
        )
    }

    fn traversal_endpoint(
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
    fn path_edge(
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

fn business_application_relationship_edge_from_record(
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
fn chain_node_sys_ids(chain: &[BusinessApplicationServerPathEdge]) -> HashSet<String> {
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
fn path_chains_contain_ancestor(
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

fn business_application_server_paths_for(
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

fn business_application_service_membership_paths_for(
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

fn merge_business_application_server_discovery(
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
fn extend_path_chains(
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

fn extended_path_chains(
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
fn emit_alternate_server_path(
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

fn servicenow_record_display_text(record: &Record, field: &str) -> Option<String> {
    record
        .get_display(field)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn mark_business_application_edge_limit(
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

fn push_business_application_server_diagnostic(
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

fn is_business_application_reference_table_resolvable(table: &str) -> bool {
    matches!(table, "sys_user" | "sys_user_group" | "cmdb_ci")
        || table == BUSINESS_APPLICATION_TABLE
        || table.contains("portfolio")
}

fn is_application_service_class(class_name: Option<&str>) -> bool {
    let Some(class_name) = class_name.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let normalized = class_name.to_ascii_lowercase();
    normalized == "cmdb_ci_service"
        || normalized.starts_with("cmdb_ci_service_")
        || normalized.contains("_application_service")
}

/// Build a cached dictionary row from a `sys_dictionary` record.
///
/// Returns `None` for rows without a usable `element` (the ServiceNow field
/// name), which can happen for collection/placeholder dictionary rows. The
/// `table_name` is the table the query was scoped to so inherited fields are
/// attributed to the level that defines them.
fn dictionary_row_from_record(
    table_name: &str,
    record: &Record,
    synced_at: DateTime<Utc>,
) -> Option<BusinessApplicationFieldDictionaryRow> {
    let field_name = non_empty_owned(record.get_raw("element"))
        .or_else(|| non_empty_owned(record.get_str("element")))?;
    let internal_type = record_field_raw_or_display(record, "internal_type");
    let reference_table = non_empty_owned(record.get_raw("reference"))
        .or_else(|| record_field_display_or_raw(record, "reference"));
    let raw_json = serde_json::json!({
        "element": field_name,
        "column_label": record_field_display_or_raw(record, "column_label"),
        "internal_type": internal_type,
        "reference": reference_table,
        "choice": record_field_raw_or_display(record, "choice"),
        "mandatory": record_field_raw_or_display(record, "mandatory"),
        "read_only": record_field_raw_or_display(record, "read_only"),
        "max_length": record_field_raw_or_display(record, "max_length"),
        "active": record_field_raw_or_display(record, "active"),
    })
    .to_string();
    Some(BusinessApplicationFieldDictionaryRow {
        table_name: table_name.to_string(),
        field_name,
        field_label: record_field_display_or_raw(record, "column_label"),
        field_type: internal_type,
        // `choice` in sys_dictionary is a numeric flag ("1"/"3" => choice list);
        // treat any non-empty, non-zero value as a choice field.
        reference_table,
        choice: dictionary_flag_is_set(record, "choice"),
        mandatory: record_bool(record, "mandatory"),
        read_only: record_bool(record, "read_only"),
        max_length: parse_i64(record_field_raw_or_display(record, "max_length").as_deref()),
        active: record_bool(record, "active"),
        synced_at,
        raw_json,
    })
}

/// Interpret a `sys_dictionary` flag field. `choice` is numeric (0/1/2/3); any
/// non-empty, non-"0" value counts as set. Boolean-style flags ("true") also
/// count.
fn dictionary_flag_is_set(record: &Record, field: &str) -> bool {
    match record_field_raw_or_display(record, field) {
        Some(value) => {
            let value = value.trim().to_ascii_lowercase();
            !value.is_empty() && value != "0" && value != "false" && value != "no"
        }
        None => false,
    }
}

/// Promote the baseline alias map to dictionary-verified fields.
///
/// For each typed product label we keep the baseline ServiceNow field name when
/// the dictionary confirms it exists, otherwise we keep the baseline (the
/// dictionary may not expose every field to the authenticated account). The
/// Primary Portfolio reference target table is taken from the dictionary
/// `reference` value when present. `dictionary_version` is set to the latest
/// `synced_at` so callers can tell typed aliases were dictionary-verified.
fn business_application_aliases_from_dictionary(
    dictionary: &HashMap<String, BusinessApplicationFieldDictionaryRow>,
) -> BusinessApplicationFieldAliases {
    let mut aliases = BusinessApplicationFieldAliases::baseline();

    // Helper: keep the baseline field name if the dictionary knows it; otherwise
    // fall back to the first dictionary field whose label matches the product
    // label. This lets instance-specific custom `u_*` fields supersede the baseline.
    let resolve = |baseline: &str, labels: &[&str]| -> String {
        if dictionary.contains_key(baseline) {
            return baseline.to_string();
        }
        dictionary
            .values()
            .find(|row| {
                row.field_label
                    .as_deref()
                    .map(|label| {
                        let label = label.trim().to_ascii_lowercase();
                        labels.iter().any(|candidate| label == *candidate)
                    })
                    .unwrap_or(false)
            })
            .map(|row| row.field_name.clone())
            .unwrap_or_else(|| baseline.to_string())
    };

    aliases.business_owner = resolve("business_owner", &["business owner"]);
    aliases.is_owner = resolve(
        "it_application_owner",
        &["is owner", "it application owner"],
    );
    aliases.ci_owner_group = resolve("managed_by_group", &["ci owner group"]);
    aliases.primary_support_group = resolve("support_group", &["primary support group"]);
    aliases.operational_state = resolve("operational_status", &["operational state"]);
    aliases.primary_portfolio = resolve("portfolio", &["primary portfolio"]);
    aliases.attested_date = resolve("attested_date", &["attested date"]);

    // Discover the Primary Portfolio reference target table from the dictionary.
    aliases.primary_portfolio_table = dictionary
        .get(&aliases.primary_portfolio)
        .and_then(|row| row.reference_table.clone())
        .filter(|table| !table.is_empty());

    aliases.dictionary_version = dictionary
        .values()
        .map(|row| row.synced_at)
        .max()
        .map(|synced_at| synced_at.to_rfc3339());

    aliases
}

fn primitive_resource_type_name(primitive_type: &ReferencePrimitiveType) -> &'static str {
    match primitive_type {
        ReferencePrimitiveType::UserPrimitive => "user_primitive",
        ReferencePrimitiveType::GroupPrimitive => "group_primitive",
        ReferencePrimitiveType::PortfolioPrimitive => "portfolio_primitive",
        ReferencePrimitiveType::ConfigurationItemPrimitive => "configuration_item_primitive",
        ReferencePrimitiveType::ReferencedRecordPrimitive => "referenced_record_primitive",
    }
}

fn primitive_status_from_reference_status(
    status: ReferenceResolutionStatus,
) -> PrimitiveResolutionStatus {
    match status {
        ReferenceResolutionStatus::Resolved => PrimitiveResolutionStatus::Resolved,
        ReferenceResolutionStatus::Unresolved => PrimitiveResolutionStatus::Unresolved,
        ReferenceResolutionStatus::UnknownTable => PrimitiveResolutionStatus::UnknownTable,
        ReferenceResolutionStatus::NotFound => PrimitiveResolutionStatus::NotFound,
        ReferenceResolutionStatus::AclRestricted => PrimitiveResolutionStatus::AclRestricted,
        ReferenceResolutionStatus::Error => PrimitiveResolutionStatus::Error,
    }
}

fn reason_from_reference_status(status: ReferenceResolutionStatus) -> ReferenceResolutionReason {
    match status {
        ReferenceResolutionStatus::UnknownTable => ReferenceResolutionReason::UnknownReferenceTable,
        ReferenceResolutionStatus::NotFound => ReferenceResolutionReason::ReferenceNotFound,
        ReferenceResolutionStatus::AclRestricted => {
            ReferenceResolutionReason::ReferenceAclRestricted
        }
        ReferenceResolutionStatus::Error => ReferenceResolutionReason::ReferenceResolutionFailed,
        ReferenceResolutionStatus::Resolved | ReferenceResolutionStatus::Unresolved => {
            ReferenceResolutionReason::DictionaryUnavailable
        }
    }
}

fn reference_resolution_status_name(status: ReferenceResolutionStatus) -> &'static str {
    match status {
        ReferenceResolutionStatus::Resolved => "resolved",
        ReferenceResolutionStatus::Unresolved => "unresolved",
        ReferenceResolutionStatus::UnknownTable => "unknown_table",
        ReferenceResolutionStatus::NotFound => "not_found",
        ReferenceResolutionStatus::AclRestricted => "acl_restricted",
        ReferenceResolutionStatus::Error => "error",
    }
}

fn primitive_display_name(record: &Record, descriptor: &ReferencePrimitiveDescriptor) -> String {
    record_first_value(
        record,
        &[
            "name",
            "display_name",
            "number",
            "user_name",
            "email",
            "title",
            "short_description",
        ],
    )
    .or_else(|| descriptor.display_value.clone())
    .unwrap_or_else(|| descriptor.reference_sys_id.clone())
}

fn record_first_value(record: &Record, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        record
            .get_display(field)
            .or_else(|| record.get_raw(field))
            .or_else(|| record.get_str(field))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn reference_primitive_relative_path(
    descriptor: &ReferencePrimitiveDescriptor,
    display_name: &str,
) -> PathBuf {
    let (dir, prefix) = match descriptor.primitive_type {
        ReferencePrimitiveType::UserPrimitive => (PathBuf::from("users"), "user".to_string()),
        ReferencePrimitiveType::GroupPrimitive => (PathBuf::from("groups"), "group".to_string()),
        ReferencePrimitiveType::PortfolioPrimitive => {
            (PathBuf::from("portfolios"), "portfolio".to_string())
        }
        ReferencePrimitiveType::ConfigurationItemPrimitive => {
            (PathBuf::from("configuration_items"), "ci".to_string())
        }
        ReferencePrimitiveType::ReferencedRecordPrimitive => {
            let table_slug = vault::layout::slugify(&descriptor.reference_table);
            (PathBuf::from("references").join(&table_slug), table_slug)
        }
    };
    let display_slug = vault::layout::slugify(display_name);
    let file_name = if display_slug.is_empty() {
        format!("{}_{}.md", prefix, descriptor.reference_sys_id)
    } else {
        format!(
            "{}_{}_{}.md",
            prefix, descriptor.reference_sys_id, display_slug
        )
    };
    dir.join(file_name)
}

fn render_reference_primitive_markdown(
    descriptor: &ReferencePrimitiveDescriptor,
    display_name: &str,
    status: ReferenceResolutionStatus,
    raw_json: &Value,
    diagnostic: Option<&str>,
) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!(
        "primitive_type: {}\n",
        yaml_json_string(primitive_resource_type_name(&descriptor.primitive_type))
    ));
    out.push_str(&format!(
        "resource_type: {}\n",
        yaml_json_string(primitive_resource_type_name(&descriptor.primitive_type))
    ));
    out.push_str(&format!(
        "sys_id: {}\n",
        yaml_json_string(&descriptor.reference_sys_id)
    ));
    out.push_str(&format!(
        "table: {}\n",
        yaml_json_string(&descriptor.reference_table)
    ));
    out.push_str(&format!(
        "display_name: {}\n",
        yaml_json_string(display_name)
    ));
    out.push_str(&format!(
        "source_field: {}\n",
        yaml_json_string(&descriptor.field)
    ));
    out.push_str(&format!(
        "resolution_status: {}\n",
        yaml_json_string(reference_resolution_status_name(status))
    ));
    if let Some(diagnostic) = diagnostic.filter(|value| !value.trim().is_empty()) {
        out.push_str(&format!("diagnostic: {}\n", yaml_json_string(diagnostic)));
    }
    out.push_str("---\n\n");
    out.push_str(&format!("# {}\n\n", display_name));
    out.push_str("```json\n");
    out.push_str(&serde_json::to_string_pretty(raw_json).unwrap_or_else(|_| raw_json.to_string()));
    out.push_str("\n```\n");
    out
}

fn primitive_projected_field(
    primitive_sys_id: &str,
    field_name: &str,
    raw_value: &Value,
    updated_at: DateTime<Utc>,
) -> ProjectedFieldRow {
    let value_text = json_field_value_text(raw_value);
    let display_value = raw_value
        .as_object()
        .and_then(|map| map.get("display_value"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let reference_sys_id = value_text
        .as_deref()
        .filter(|value| looks_like_servicenow_sys_id(value) && display_value.is_some())
        .map(ToOwned::to_owned);
    let reference_table = raw_value
        .as_object()
        .and_then(|map| map.get("link"))
        .and_then(Value::as_str)
        .and_then(reference_table_from_api_link);
    let number_text = value_text
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok());
    let bool_value =
        value_text
            .as_deref()
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            });
    let date_value = value_text
        .as_deref()
        .and_then(|value| value.get(..10))
        .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
        .map(|date| date.to_string());

    ProjectedFieldRow {
        owner_sys_id: primitive_sys_id.to_string(),
        field_name: field_name.to_string(),
        field_label: None,
        field_type: reference_sys_id.as_ref().map(|_| "reference".to_string()),
        value_text,
        display_value,
        value_number: number_text,
        value_date: date_value,
        value_bool: bool_value,
        reference_sys_id,
        reference_table,
        raw_json: raw_value.to_string(),
        updated_at,
    }
}

fn json_field_value_text(value: &Value) -> Option<String> {
    let scalar = value
        .as_object()
        .and_then(|map| map.get("value"))
        .unwrap_or(value);
    match scalar {
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

fn reference_table_from_api_link(link: &str) -> Option<String> {
    let marker = "/api/now/table/";
    let start = link.find(marker)? + marker.len();
    let table = link[start..].split('/').next()?.trim();
    (!table.is_empty()).then(|| table.to_string())
}

fn looks_like_servicenow_sys_id(value: &str) -> bool {
    let value = value.trim();
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn yaml_json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

fn push_unique_reference_diagnostic(
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    diagnostic: ReferenceResolutionDiagnostic,
) {
    if diagnostics.iter().any(|existing| {
        existing.field == diagnostic.field
            && existing.reference_table == diagnostic.reference_table
            && existing.reference_sys_id == diagnostic.reference_sys_id
            && existing.reason == diagnostic.reason
    }) {
        return;
    }
    diagnostics.push(diagnostic);
}

#[derive(Clone)]
pub(crate) struct BusinessApplicationService {
    ctx: CoreContext,
}

impl BusinessApplicationService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn search_business_applications(
        &self,
        params: BusinessApplicationSearchParams,
    ) -> Result<Vec<SnowRecord>> {
        Ok(self
            .search_business_applications_live(
                params,
                BusinessApplicationHydrationOptions::default(),
            )
            .await?
            .into_iter()
            .map(|business_application| business_application.record)
            .collect())
    }

    pub async fn get_business_application_fresh(
        &self,
        lookup: BusinessApplicationLookup,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<Option<BusinessApplication>> {
        let aliases = self
            .resolve_business_application_aliases(options.refresh_dictionary)
            .await;
        let record = match lookup {
            BusinessApplicationLookup::SysId(sys_id) => match self
                .ctx
                .client
                .table(BUSINESS_APPLICATION_TABLE)
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(&normalize_record_lookup_sys_id(&sys_id)?)
                .await
            {
                Ok(record) => Some(record),
                Err(SnowApiError::Api { status: 404, .. }) => None,
                Err(err) => return Err(err.into()),
            },
            BusinessApplicationLookup::ExactName(name) => {
                let name = non_empty_owned(Some(&name))
                    .ok_or_else(|| anyhow::anyhow!("Business Application name cannot be empty"))?;
                let records = self
                    .ctx
                    .client
                    .table(BUSINESS_APPLICATION_TABLE)
                    .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
                    .equals("name", &name)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .limit(2)
                    .execute()
                    .await?
                    .records;
                if records.len() > 1 {
                    anyhow::bail!("multiple Business Applications matched name={name}");
                }
                records.into_iter().next()
            }
        };

        let Some(record) = record else {
            return Ok(None);
        };
        let mut business_application = BusinessApplication::from_servicenow(&record, &aliases)?;
        if options.persist {
            self.ctx.persist_record(&record)?;
            self.persist_business_application_reference_primitives(
                &mut business_application,
                &options,
            )
            .await?;
        }
        Ok(Some(business_application))
    }

    pub async fn search_business_applications_live(
        &self,
        params: BusinessApplicationSearchParams,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<Vec<BusinessApplication>> {
        params.validate()?;
        let aliases = self
            .resolve_business_application_aliases(options.refresh_dictionary)
            .await;

        let mut query = self
            .ctx
            .client
            .table(BUSINESS_APPLICATION_TABLE)
            .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(params.validated_limit()? as u32)
            .order_by("name", Order::Asc);

        if let Some(name) = non_empty_owned(params.name.as_deref()) {
            query = query.contains("name", &name);
        }
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.business_owner,
            params.business_owner.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.is_owner,
            params.is_owner.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.ci_owner_group,
            params.ci_owner_group.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.primary_support_group,
            params.primary_support_group.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.primary_portfolio,
            params.primary_portfolio.as_deref(),
        )?;
        if let Some(status) = non_empty_owned(params.operational_state.as_deref()) {
            query = query.equals(
                &aliases.operational_state,
                &normalize_operational_state(&status),
            );
        }
        if let Some(status) = non_empty_owned(params.operational_state_not.as_deref()) {
            query = query.not_equals(
                &aliases.operational_state,
                &normalize_operational_state(&status),
            );
        }
        if let Some(date) = non_empty_owned(params.attested_date.as_deref()) {
            query = query.equals(&aliases.attested_date, &date);
        }
        if let Some(date) = non_empty_owned(params.attested_date_on_or_after.as_deref()) {
            query = query.filter(&aliases.attested_date, Operator::GreaterThanOrEqual, &date);
        }
        if let Some(date) = non_empty_owned(params.attested_date_on_or_before.as_deref()) {
            query = query.filter(&aliases.attested_date, Operator::LessThanOrEqual, &date);
        }

        let records = query.execute().await?.records;
        self.hydrate_business_application_page(records, &aliases, &options)
            .await
    }

    pub async fn query_business_applications(
        &self,
        query: BusinessApplicationQuery,
    ) -> Result<Vec<SnowRecord>> {
        self.ctx.query.query_business_applications(query).await
    }

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
    async fn business_application_servers_with_options(
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
    async fn business_application_ci_owner_group_fallback(
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

    #[allow(clippy::too_many_arguments)]
    fn persist_business_application_server_traversal(
        &self,
        application: &SnowRecord,
        servers: &[Server],
        server_provenance: &BTreeMap<String, BusinessApplicationServerProvenance>,
        server_paths: &BTreeMap<String, Vec<BusinessApplicationServerPath>>,
        run_started_at: DateTime<Utc>,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
    ) -> Result<()> {
        self.ctx
            .persist_snow_records(std::slice::from_ref(application))?;
        let server_records = servers
            .iter()
            .map(|server| server.record.clone())
            .collect::<Vec<_>>();
        summary.persisted_servers = self.ctx.persist_snow_records(&server_records)?;

        let store = self.ctx.query.store();
        let now = Utc::now();
        for server in servers {
            let paths = server_paths
                .get(&server.record.sys_id)
                .cloned()
                .unwrap_or_default();
            let min_depth = paths
                .iter()
                .map(BusinessApplicationServerPath::depth)
                .min()
                .unwrap_or(0);
            let provenance = server_provenance
                .get(&server.record.sys_id)
                .copied()
                .unwrap_or(BusinessApplicationServerProvenance::Relationship);
            let server_table = server
                .class_name
                .as_deref()
                .and_then(|value| non_empty_owned(Some(value)))
                .or_else(|| non_empty_owned(Some(server.record.table.as_str())))
                .unwrap_or_else(|| SERVER_TABLE.to_string());
            let membership = BusinessApplicationServerMembershipRow {
                ba_sys_id: application.sys_id.clone(),
                server_sys_id: server.record.sys_id.clone(),
                server_table,
                provenance: provenance.as_str().to_string(),
                min_depth,
                paths_json: serde_json::to_string(&paths)?,
                discovered_at: now,
                last_seen_at: now,
                tombstoned_at: None,
            };
            store.upsert_business_application_server_membership(&membership)?;
            summary.membership_upserts += 1;
        }

        if options.prune_stale {
            summary.membership_pruned = store
                .tombstone_stale_business_application_server_memberships(
                    application.sys_id.as_str(),
                    run_started_at,
                    Utc::now(),
                )?;
        }
        let run_completed_at = Utc::now();
        let service_membership_status =
            Self::business_application_service_membership_health_status(summary);
        let relationship_status = Self::business_application_relationship_health_status(summary);
        let inventory_status = Self::business_application_inventory_health_status(
            relationship_status,
            &service_membership_status,
            summary,
        );
        summary.service_membership_status = Some(service_membership_status.to_string());
        summary.relationship_status = Some(relationship_status.to_string());
        summary.inventory_status = Some(inventory_status.to_string());
        store.upsert_business_application_server_inventory_health(
            &BusinessApplicationServerInventoryHealthRow {
                ba_sys_id: application.sys_id.clone(),
                run_started_at,
                run_completed_at,
                service_membership_status: service_membership_status.to_string(),
                relationship_status: relationship_status.to_string(),
                inventory_status: inventory_status.to_string(),
                summary_json: serde_json::to_string(summary)?,
            },
        )?;
        Ok(())
    }

    pub async fn get_business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        self.business_application_servers(params).await
    }

    fn business_application_relationship_health_status(
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

    fn business_application_service_membership_health_status(
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

    fn business_application_inventory_health_status(
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
    async fn resolve_relationship_type_allowlist(
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

    async fn resolve_business_application_servers_selector(
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

    async fn business_application_relationship_level(
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
    fn merge_business_application_direction_read(
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
    async fn business_application_relationship_direction_read(
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

    async fn business_application_service_membership_servers(
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

    async fn business_application_service_membership_read(
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
    async fn business_application_class_is_server(
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

    async fn business_application_hydrate_ci_classes(
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

    async fn business_application_hydrate_servers(
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
    /// Run a live Business Application search+persist and aggregate a sync summary.
    ///
    /// Reuses [`Self::search_business_applications_live`] for the actual fetch and
    /// persistence (honoring `options.persist`), then rolls up reference
    /// resolution and dictionary-degradation status across the returned
    /// applications. `params` is optional; `None` produces the default bounded
    /// Business Application search ordered by `name`.
    ///
    /// This is a degraded-tolerant read: unresolved references and an
    /// unavailable dictionary are reported in the summary, never errors.
    pub async fn sync_business_applications(
        &self,
        params: Option<BusinessApplicationSearchParams>,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        let params = params.unwrap_or_default();
        let page_size = params.validated_limit()?;
        // When the caller explicitly asks for a dictionary refresh, attempt it
        // up front so degraded status reflects the freshest metadata. The fetch
        // is best-effort: a failure leaves us in baseline/degraded mode. We do
        // the refresh here (and clear the flag for the live search) so it runs
        // exactly once per sync.
        let mut dictionary_refreshed = false;
        let mut live_options = options.clone();
        if options.refresh_dictionary {
            dictionary_refreshed = self.refresh_business_application_dictionary().await.is_ok();
            live_options.refresh_dictionary = false;
        }

        let applications = self
            .search_business_applications_live(params, live_options)
            .await?;

        let mut summary = BusinessApplicationSyncSummary {
            all: false,
            table: BUSINESS_APPLICATION_TABLE.to_string(),
            page_size,
            pages: usize::from(!applications.is_empty()),
            total_returned: applications.len(),
            total_applications: applications.len(),
            persisted: if options.persist {
                applications.len()
            } else {
                0
            },
            dictionary_refreshed,
            ..Default::default()
        };

        roll_up_business_application_summary(&mut summary, &applications);

        Ok(summary)
    }

    /// Drain every live Business Application page and persist each page before
    /// requesting the next one.
    ///
    /// This explicit full-inventory path uses the `servicenow_rs` paginator
    /// directly. It deliberately avoids `execute_all()` so persistence remains
    /// durable page by page and the whole live result set is never required in
    /// memory.
    pub async fn sync_all_business_applications(
        &self,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        let mut dictionary_refreshed = false;
        let mut live_options = options.clone();
        if options.refresh_dictionary {
            dictionary_refreshed = self.refresh_business_application_dictionary().await.is_ok();
            live_options.refresh_dictionary = false;
        }

        let aliases = self
            .resolve_business_application_aliases(live_options.refresh_dictionary)
            .await;
        let mut paginator = self
            .ctx
            .client
            .table(BUSINESS_APPLICATION_TABLE)
            .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE as u32)
            .order_by("name", Order::Asc)
            .order_by("sys_id", Order::Asc)
            .paginate()?;

        let mut summary = BusinessApplicationSyncSummary {
            all: true,
            table: BUSINESS_APPLICATION_TABLE.to_string(),
            page_size: BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE,
            dictionary_refreshed,
            ..Default::default()
        };

        while let Some(page) = paginator.next_page().await? {
            if page.records.is_empty() {
                continue;
            }
            let applications = self
                .hydrate_business_application_page(page.records, &aliases, &live_options)
                .await?;

            summary.pages += 1;
            summary.total_returned += applications.len();
            summary.total_applications += applications.len();
            if options.persist {
                summary.persisted += applications.len();
            }
            roll_up_business_application_summary(&mut summary, &applications);
        }

        Ok(summary)
    }

    async fn hydrate_business_application_page(
        &self,
        records: Vec<Record>,
        aliases: &BusinessApplicationFieldAliases,
        options: &BusinessApplicationHydrationOptions,
    ) -> Result<Vec<BusinessApplication>> {
        let mut business_applications = Vec::with_capacity(records.len());
        for record in records {
            let mut business_application = BusinessApplication::from_servicenow(&record, aliases)?;
            if options.persist {
                self.ctx.persist_record(&record)?;
                self.persist_business_application_reference_primitives(
                    &mut business_application,
                    options,
                )
                .await?;
            }
            business_applications.push(business_application);
        }
        Ok(business_applications)
    }

    async fn persist_business_application_reference_primitives(
        &self,
        business_application: &mut BusinessApplication,
        options: &BusinessApplicationHydrationOptions,
    ) -> Result<()> {
        if !options.persist {
            return Ok(());
        }

        let mut diagnostics = Vec::new();
        let max_direct_references = 100usize;
        let total_references = business_application.references.len();

        for descriptor in business_application
            .references
            .iter_mut()
            .take(max_direct_references)
        {
            let can_resolve = options.resolve_references
                && options.reference_depth > 0
                && is_business_application_reference_table_resolvable(
                    descriptor.reference_table.as_str(),
                );

            if !can_resolve {
                if descriptor.resolution_status == ReferenceResolutionStatus::Resolved {
                    descriptor.resolution_status =
                        if is_business_application_reference_table_resolvable(
                            descriptor.reference_table.as_str(),
                        ) {
                            ReferenceResolutionStatus::Unresolved
                        } else {
                            ReferenceResolutionStatus::UnknownTable
                        };
                }
                let diagnostic = self.persist_reference_primitive_stub(descriptor, None)?;
                diagnostics.push(diagnostic);
                continue;
            }

            match self
                .ctx
                .client
                .table(descriptor.reference_table.as_str())
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(descriptor.reference_sys_id.as_str())
                .await
            {
                Ok(record) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::Resolved;
                    descriptor.diagnostic = None;
                    self.persist_resolved_reference_primitive(descriptor, &record)?;
                }
                Err(SnowApiError::Api { status: 404, .. }) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::NotFound;
                    descriptor.diagnostic = Some("referenced record was not found".to_string());
                    diagnostics.push(self.persist_reference_primitive_stub(descriptor, None)?);
                }
                Err(err) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::Error;
                    let message = err.to_string();
                    descriptor.diagnostic = Some(message.clone());
                    diagnostics
                        .push(self.persist_reference_primitive_stub(descriptor, Some(message))?);
                }
            }
        }

        if total_references > max_direct_references {
            diagnostics.push(ReferenceResolutionDiagnostic {
                field: "*".to_string(),
                reference_table: "*".to_string(),
                reference_sys_id: business_application.record.sys_id.clone(),
                display_value: Some(business_application.name.clone()),
                reason: ReferenceResolutionReason::FanoutLimitExceeded,
                message: Some(format!(
                    "resolved first {max_direct_references} Business Application references out of {total_references}"
                )),
            });
        }

        for diagnostic in diagnostics {
            push_unique_reference_diagnostic(
                &mut business_application.unresolved_references,
                diagnostic,
            );
        }

        Ok(())
    }

    fn persist_resolved_reference_primitive(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        record: &Record,
    ) -> Result<()> {
        let raw_json = serialize_record_document(record);
        let display_name = primitive_display_name(record, descriptor);
        let relative_path = self.persist_reference_primitive_markdown(
            descriptor,
            &display_name,
            ReferenceResolutionStatus::Resolved,
            &raw_json,
            None,
        )?;
        let synced_at = Utc::now();
        self.ctx
            .query
            .store()
            .upsert_primitive_object(&PrimitiveObjectRow {
                sys_id: descriptor.reference_sys_id.clone(),
                table_name: descriptor.reference_table.clone(),
                resource_type: primitive_resource_type_name(&descriptor.primitive_type).to_string(),
                display_name,
                number: record_first_value(record, &["number", "user_name"]).or_else(|| {
                    record
                        .get_raw("number")
                        .or_else(|| record.get_display("number"))
                        .map(ToOwned::to_owned)
                }),
                file_path: Some(relative_path.to_string_lossy().into_owned()),
                raw_json: raw_json.to_string(),
                synced_at,
                sys_updated_on: record
                    .get_raw("sys_updated_on")
                    .or_else(|| record.get_display("sys_updated_on"))
                    .or_else(|| record.get_str("sys_updated_on"))
                    .map(ToOwned::to_owned),
                resolution_status: PrimitiveResolutionStatus::Resolved,
                last_error: None,
            })?;

        if let Some(raw_map) = raw_json.as_object() {
            for (field_name, raw_value) in raw_map {
                let field = primitive_projected_field(
                    &descriptor.reference_sys_id,
                    field_name,
                    raw_value,
                    synced_at,
                );
                self.ctx
                    .query
                    .store()
                    .upsert_primitive_object_field(&descriptor.reference_sys_id, &field)?;
            }
        }
        Ok(())
    }

    fn persist_reference_primitive_stub(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        last_error: Option<String>,
    ) -> Result<ReferenceResolutionDiagnostic> {
        let status = descriptor.resolution_status;
        let display_name = descriptor
            .display_value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(descriptor.reference_sys_id.as_str())
            .to_string();
        let raw_json = serde_json::json!({
            "sys_id": descriptor.reference_sys_id,
            "table": descriptor.reference_table,
            "display_value": display_name,
            "source_field": descriptor.field,
            "resolution_status": reference_resolution_status_name(status),
            "diagnostic": descriptor.diagnostic.as_deref().or(last_error.as_deref()),
        });
        let relative_path = self.persist_reference_primitive_markdown(
            descriptor,
            &display_name,
            status,
            &raw_json,
            descriptor.diagnostic.as_deref().or(last_error.as_deref()),
        )?;
        self.ctx
            .query
            .store()
            .upsert_primitive_object(&PrimitiveObjectRow {
                sys_id: descriptor.reference_sys_id.clone(),
                table_name: descriptor.reference_table.clone(),
                resource_type: primitive_resource_type_name(&descriptor.primitive_type).to_string(),
                display_name: display_name.clone(),
                number: None,
                file_path: Some(relative_path.to_string_lossy().into_owned()),
                raw_json: raw_json.to_string(),
                synced_at: Utc::now(),
                sys_updated_on: None,
                resolution_status: primitive_status_from_reference_status(status),
                last_error,
            })?;

        Ok(ReferenceResolutionDiagnostic {
            field: descriptor.field.clone(),
            reference_table: descriptor.reference_table.clone(),
            reference_sys_id: descriptor.reference_sys_id.clone(),
            display_value: descriptor.display_value.clone(),
            reason: reason_from_reference_status(status),
            message: descriptor.diagnostic.clone(),
        })
    }

    fn persist_reference_primitive_markdown(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        display_name: &str,
        status: ReferenceResolutionStatus,
        raw_json: &Value,
        diagnostic: Option<&str>,
    ) -> Result<PathBuf> {
        let relative_path = reference_primitive_relative_path(descriptor, display_name);
        let absolute_path = self.ctx.vault_path.join(&relative_path);
        let contents = render_reference_primitive_markdown(
            descriptor,
            display_name,
            status,
            raw_json,
            diagnostic,
        );
        let persisted = self
            .ctx
            .vault
            .write_markdown_file(absolute_path, &contents)?;
        Ok(persisted.relative_path)
    }
    /// The Business Application table and all of its inherited tables, most
    /// derived first. Used to scope `sys_dictionary` queries and dictionary
    /// cache lookups. Inheritance traversal is bounded to 8 levels by
    /// [`Self::table_ancestors`].
    async fn business_application_dictionary_tables(&self) -> Result<Vec<String>> {
        let mut tables = vec![BUSINESS_APPLICATION_TABLE.to_string()];
        tables.extend(self.ctx.table_ancestors(BUSINESS_APPLICATION_TABLE).await?);
        Ok(tables)
    }

    /// Fetch live `sys_dictionary` metadata for `cmdb_ci_business_app` and its
    /// inherited tables, then upsert the active rows into the
    /// `business_application_field_dictionary` cache.
    ///
    /// Returns the number of dictionary rows persisted. A failure to reach the
    /// dictionary (or an empty result) is surfaced as an error/zero so callers
    /// can stay in degraded-read mode; it must never abort a normal BA read.
    pub async fn refresh_business_application_dictionary(&self) -> Result<usize> {
        let tables = self.business_application_dictionary_tables().await?;
        let synced_at = Utc::now();
        let mut persisted = 0usize;
        for table in &tables {
            // One query per table keeps each `name=<table>` scoped and lets a
            // single failing table degrade independently of the others.
            let records = self
                .ctx
                .client
                .table("sys_dictionary")
                .equals("name", table)
                .equals("active", "true")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .limit(2000)
                .execute()
                .await?
                .records;
            for record in records {
                let Some(row) = dictionary_row_from_record(table, &record, synced_at) else {
                    continue;
                };
                self.ctx
                    .query
                    .store()
                    .upsert_business_application_field_dictionary(&row)?;
                persisted += 1;
            }
        }
        Ok(persisted)
    }

    /// Read the cached, dictionary-verified field metadata for the Business
    /// Application table and its ancestors, keyed by ServiceNow field name.
    ///
    /// Returns an empty map on a dictionary cache miss (degraded-read mode).
    pub async fn business_application_dictionary(
        &self,
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        let tables = self.business_application_dictionary_tables().await?;
        Ok(self
            .ctx
            .query
            .store()
            .business_application_dictionary_for_tables(&tables)?)
    }

    /// Build the typed alias map for the Business Application primitive,
    /// promoting baseline aliases to dictionary-verified fields when cached
    /// `sys_dictionary` metadata is present.
    ///
    /// On a dictionary cache miss this returns
    /// [`BusinessApplicationFieldAliases::baseline_degraded`], which carries a
    /// `DictionaryUnavailable` diagnostic so the degradation is never silent.
    pub async fn business_application_aliases(&self) -> Result<BusinessApplicationFieldAliases> {
        let dictionary = self.business_application_dictionary().await?;
        if dictionary.is_empty() {
            return Ok(BusinessApplicationFieldAliases::baseline_degraded());
        }
        Ok(business_application_aliases_from_dictionary(&dictionary))
    }

    /// Resolve the Business Application alias map for a hydration run, optionally
    /// refreshing the dictionary first.
    ///
    /// When `refresh_dictionary` is set, a best-effort live dictionary fetch runs
    /// before resolving so freshly verified instance field names take effect. A
    /// failure to refresh or an empty cache yields the degraded baseline aliases
    /// (carrying a `DictionaryUnavailable` diagnostic) so reads never fail.
    async fn resolve_business_application_aliases(
        &self,
        refresh_dictionary: bool,
    ) -> BusinessApplicationFieldAliases {
        if refresh_dictionary {
            let _ = self.refresh_business_application_dictionary().await;
        }
        self.business_application_aliases()
            .await
            .unwrap_or_else(|_| BusinessApplicationFieldAliases::baseline_degraded())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceType;
    use crate::resource::business_application::{
        BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED,
        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
        BusinessApplicationServersCachedParams, BusinessApplicationsForServerParams,
        RelationshipKnowledgeStatus,
    };
    use crate::tests::{core_for_mock_server, mock_server_test_lock};
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn business_application_search_builds_supported_filters() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "short_description": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": { "value": "owner-sys", "display_value": "Jane Owner" },
                    "it_application_owner": { "value": "is-owner-sys", "display_value": "Alex IS" },
                    "managed_by_group": { "value": "ci-group-sys", "display_value": "CI Owners" },
                    "support_group": { "value": "support-group-sys", "display_value": "App Support" },
                    "operational_status": { "value": "1", "display_value": "Operational" },
                    "portfolio": { "value": "portfolio-sys", "display_value": "Clinical" },
                    "attested_date": "2026-05-01",
                    "u_custom_field": { "value": "raw-custom", "display_value": "Custom Display" },
                    "u_json_blob": { "value": { "kept": true }, "display_value": "Kept" }
                }]
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let records = core
            .search_business_applications(BusinessApplicationSearchParams {
                name: Some("Epic".to_string()),
                business_owner: Some("Jane Owner".to_string()),
                is_owner: Some("Alex IS".to_string()),
                ci_owner_group: Some("CI Owners".to_string()),
                primary_support_group: Some("App Support".to_string()),
                operational_state_not: Some("non-operational".to_string()),
                primary_portfolio: Some("Clinical".to_string()),
                attested_date: Some("2026-05-01".to_string()),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .expect("business applications");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].number, "BA:54a4b61b6fe845000ed852a03f3ee4d0");
        assert_eq!(records[0].table, "cmdb_ci_business_app");
        assert_eq!(records[0].resource_type, ResourceType::BusinessApplication);
        assert_eq!(records[0].fields["name"].value, "Epic");
        assert_eq!(records[0].fields["u_custom_field"].value, "raw-custom");
        assert_eq!(records[0].fields["u_json_blob"].value, "{\"kept\":true}");
        assert!(
            tempdir
                .path()
                .join("vault/business_applications/business_application_54a4b61b6fe845000ed852a03f3ee4d0_epic.md")
                .exists()
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/cmdb_ci_business_app")
            .expect("business app request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("sysparm_query").map(|value| value.as_ref()),
            Some(
                "sys_class_name=cmdb_ci_business_app^nameLIKEEpic^business_owner.nameLIKEJane Owner^it_application_owner.nameLIKEAlex IS^managed_by_group.nameLIKECI Owners^support_group.nameLIKEApp Support^portfolio.nameLIKEClinical^operational_status!=2^attested_date=2026-05-01^ORDERBYname"
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
            Some("2")
        );
    }

    #[tokio::test]
    async fn business_application_search_persists_resolved_reference_primitives() {
        let server = MockServer::start().await;
        let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": {
                        "value": owner_sys_id,
                        "display_value": "Jane Owner"
                    },
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sys_user/{owner_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": owner_sys_id,
                    "name": "Jane Owner",
                    "user_name": "jowner",
                    "email": "jane.owner@example.invalid",
                    "active": { "value": "true", "display_value": "true" },
                    "sys_updated_on": "2026-05-30 12:00:00"
                }
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let applications = core
            .search_business_applications_live(
                BusinessApplicationSearchParams {
                    name: Some("Epic".to_string()),
                    limit: Some(1),
                    ..Default::default()
                },
                BusinessApplicationHydrationOptions::default(),
            )
            .await
            .expect("business applications");

        assert_eq!(applications.len(), 1);
        assert!(
            applications[0]
                .unresolved_references
                .iter()
                .all(|diagnostic| { diagnostic.reference_sys_id != owner_sys_id })
        );
        let primitive = core
            .ctx
            .query
            .store()
            .get_primitive_object(owner_sys_id)
            .expect("primitive lookup")
            .expect("primitive object");
        assert_eq!(primitive.table_name, "sys_user");
        assert_eq!(primitive.resource_type, "user_primitive");
        assert_eq!(primitive.display_name, "Jane Owner");
        assert_eq!(
            primitive.resolution_status,
            PrimitiveResolutionStatus::Resolved
        );
        let file_path = primitive.file_path.expect("primitive vault path");
        assert!(file_path.starts_with("users/user_6816f79cc0a8016401c5a33be04be441_jane-owner.md"));
        assert!(tempdir.path().join("vault").join(file_path).exists());
    }

    #[tokio::test]
    async fn business_application_fresh_get_omits_sysparm_fields() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "operational_status": { "value": "1", "display_value": "Operational" },
                    "u_observed": "yes"
                }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let business_application = core
            .get_business_application_fresh(
                BusinessApplicationLookup::sys_id("54A4B61B6FE845000ED852A03F3EE4D0").unwrap(),
                BusinessApplicationHydrationOptions::default(),
            )
            .await
            .expect("fresh get")
            .expect("business application");

        assert_eq!(business_application.name, "Epic");
        assert_eq!(
            business_application.record.number,
            "BA:54a4b61b6fe845000ed852a03f3ee4d0"
        );
        assert_eq!(
            business_application.fields["u_observed"].value,
            serde_json::json!("yes")
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| {
                request.url.path()
                    == "/api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0"
            })
            .expect("business app get request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(query.get("sysparm_fields"), None);
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
    }

    #[test]
    fn business_application_servers_params_validate_selector_and_bounds() {
        let options = BusinessApplicationServersParams {
            number: Some("apm0000001".to_string()),
            ..Default::default()
        }
        .validate()
        .expect("valid number selector");
        assert_eq!(
            options.selector,
            BusinessApplicationServersSelector::Number("APM0000001".to_string())
        );
        assert_eq!(options.max_depth, 2);
        assert_eq!(options.max_cis, 500);
        assert_eq!(options.max_edges, 2000);

        let err = BusinessApplicationServersParams::default()
            .validate()
            .expect_err("missing selector should fail")
            .to_string();
        assert!(err.contains("exactly one"));

        let err = BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            sys_id: Some("11111111111111111111111111111111".to_string()),
            ..Default::default()
        }
        .validate()
        .expect_err("dual selector should fail")
        .to_string();
        assert!(err.contains("exactly one"));

        let err = BusinessApplicationServersParams {
            number: Some("BA:11111111111111111111111111111111".to_string()),
            ..Default::default()
        }
        .validate()
        .expect_err("synthetic BA number should fail")
        .to_string();
        assert!(err.contains("BA:<sys_id>"));

        let err = BusinessApplicationServersParams {
            number: Some("APP0000001".to_string()),
            ..Default::default()
        }
        .validate()
        .expect_err("non-APM number should fail")
        .to_string();
        assert!(err.contains("<APM_NUMBER>"));

        let err = BusinessApplicationServersParams {
            sys_id: Some("11111111111111111111111111111111".to_string()),
            max_depth: Some(5),
            ..Default::default()
        }
        .validate()
        .expect_err("max depth should be bounded")
        .to_string();
        assert!(err.contains("at most 4"));
    }

    #[test]
    fn business_application_server_path_chains_reject_mixed_directions() {
        let root = "11111111111111111111111111111111";
        let middle = "22222222222222222222222222222222";
        let leaf = "33333333333333333333333333333333";
        let rel_type = BusinessApplicationRelationshipType {
            value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            display_value: Some("Depends on::Used by".to_string()),
        };
        let mut paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
            HashMap::from([(root.to_string(), vec![Vec::new()])]);

        assert!(extend_path_chains(
            &mut paths_by_ci,
            root,
            middle,
            BusinessApplicationServerPathEdge {
                depth: 1,
                parent_sys_id: root.to_string(),
                child_sys_id: middle.to_string(),
                direction: BusinessApplicationRelationshipDirection::ParentToChild,
                relationship_type: rel_type.clone(),
                edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
            },
        ));

        assert!(!extend_path_chains(
            &mut paths_by_ci,
            middle,
            leaf,
            BusinessApplicationServerPathEdge {
                depth: 2,
                parent_sys_id: leaf.to_string(),
                child_sys_id: middle.to_string(),
                direction: BusinessApplicationRelationshipDirection::ChildToParent,
                relationship_type: rel_type,
                edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
            },
        ));
        assert!(!paths_by_ci.contains_key(leaf));
    }

    #[tokio::test]
    async fn business_application_servers_batches_bfs_levels_and_hydrates_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let windows = "44444444444444444444444444444444";
        let component = "55555555555555555555555555555555";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rel_row_1 = "99999999999999999999999999999991";
        let rel_row_2 = "99999999999999999999999999999992";
        let rel_row_3 = "99999999999999999999999999999993";
        let rel_row_4 = "99999999999999999999999999999994";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app",
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(rel_row_1, app, service, rel_type, "cmdb_ci_business_app", "cmdb_ci_service"),
                    relationship_row(rel_row_2, app, component, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row(rel_row_3, app, linux, rel_type, "cmdb_ci_business_app", "cmdb_ci_linux_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{service},{component}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(rel_row_4, component, windows, rel_type, "cmdb_ci_appl", "cmdb_ci_win_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("childIN{service},{component}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{windows}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(windows, "windows-alpha.example.com", "cmdb_ci_win_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.business_application.sys_id, app);
        assert_eq!(result.servers.len(), 2);
        assert_eq!(result.relationship_summary.relationships_examined, 4);
        assert_eq!(result.relationship_summary.cis_examined, 5);
        assert_eq!(result.relationship_summary.servers_found, 2);
        assert_eq!(result.relationship_summary.persisted_servers, 2);
        assert_eq!(result.relationship_summary.membership_upserts, 2);
        assert_eq!(result.relationship_summary.membership_pruned, 0);
        assert!(!result.relationship_summary.truncated);
        assert_eq!(result.server_paths[linux][0].edges.len(), 1);
        assert_eq!(result.server_paths[windows][0].edges.len(), 2);
        assert_eq!(
            result.server_paths[linux][0].edges[0].direction,
            BusinessApplicationRelationshipDirection::ParentToChild
        );

        let serialized = serde_json::to_string(&result).expect("serialize result");
        assert!(!serialized.contains(rel_row_1));
        assert!(!serialized.contains(rel_row_2));
        assert!(!serialized.contains(rel_row_3));
        assert!(!serialized.contains(rel_row_4));

        let cached_forward = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(app.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward lookup")
            .expect("business application cached");
        assert_eq!(cached_forward.business_application.sys_id, app);
        assert_eq!(cached_forward.servers.len(), 2);
        assert_eq!(cached_forward.servers[0].server.sys_id, linux);
        assert_eq!(cached_forward.servers[0].provenance, "relationship");
        assert_eq!(cached_forward.servers[0].min_depth, 1);
        assert_eq!(cached_forward.servers[0].paths[0].depth(), 1);
        assert_eq!(cached_forward.servers[1].server.sys_id, windows);
        assert_eq!(cached_forward.servers[1].min_depth, 2);
        assert_eq!(cached_forward.servers[1].paths[0].depth(), 2);
        assert_eq!(
            cached_forward.relationship_status,
            RelationshipKnowledgeStatus::KnownRelationships
        );
        let forward_health = cached_forward
            .inventory_health
            .as_ref()
            .expect("forward inventory health");
        assert_eq!(forward_health.ba_sys_id, app);
        assert_eq!(forward_health.service_membership_status, "not_attempted");
        assert_eq!(forward_health.relationship_status, "ok");
        assert_eq!(forward_health.inventory_status, "complete");

        let cached_reverse = core
            .business_applications_for_server(BusinessApplicationsForServerParams {
                name: Some("linux-alpha.example.com".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached reverse lookup")
            .expect("server cached");
        assert_eq!(cached_reverse.servers.len(), 1);
        assert_eq!(cached_reverse.servers[0].server.sys_id, linux);
        assert_eq!(cached_reverse.servers[0].business_applications.len(), 1);
        assert_eq!(
            cached_reverse.servers[0].business_applications[0]
                .business_application
                .sys_id,
            app
        );
        assert_eq!(
            cached_reverse.servers[0].relationship_status,
            RelationshipKnowledgeStatus::KnownRelationships
        );
        assert_eq!(
            cached_reverse.servers[0].business_applications[0]
                .inventory_health
                .as_ref()
                .expect("reverse inventory health")
                .inventory_status,
            "complete"
        );

        let requests = server.received_requests().await.expect("requests");
        let relationship_queries = requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/cmdb_rel_ci")
            .filter_map(|request| {
                request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "sysparm_query")
                    .map(|(_, value)| value.to_string())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            relationship_queries,
            vec![
                format!("parentIN{app}"),
                format!("childIN{app}"),
                format!("parentIN{service},{component}"),
                format!("childIN{service},{component}"),
            ]
        );
    }

    /// Fix #3 regression guard: in a diamond topology (`app -> A -> S` and
    /// `app -> B -> S`) the server `S` is reachable via two distinct parents.
    /// With `include_paths`, BOTH routes must be recorded (the plural
    /// `server_paths` Vec holds two entries), while `S` remains a single server
    /// result. The second discovery is an alternate forward path, not a cycle.
    #[tokio::test]
    async fn business_application_servers_records_diamond_alternate_paths() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let branch_a = "22222222222222222222222222222222";
        let branch_b = "33333333333333333333333333333333";
        let leaf = "44444444444444444444444444444444";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Depth 1: app -> A and app -> B (two intermediate application CIs).
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, branch_a, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row("99999999999999999999999999999992", app, branch_b, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Depth 2: both A and B point at the SAME leaf server S.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{branch_a},{branch_b}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999993", branch_a, leaf, rel_type, "cmdb_ci_appl", "cmdb_ci_linux_server"),
                    relationship_row("99999999999999999999999999999994", branch_b, leaf, rel_type, "cmdb_ci_appl", "cmdb_ci_linux_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("childIN{branch_a},{branch_b}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{leaf}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(leaf, "linux-leaf.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        // One server, two distinct routes to it.
        assert_eq!(result.servers.len(), 1);
        assert_eq!(result.servers[0].record.sys_id, leaf);
        let paths = result
            .server_paths
            .get(leaf)
            .expect("leaf should have recorded paths");
        assert_eq!(paths.len(), 2, "both diamond routes must be recorded");
        // Each route is two edges long: app->branch then branch->leaf.
        assert!(paths.iter().all(|route| route.edges.len() == 2));
        // The two routes go through different branch CIs.
        let mut branches = paths
            .iter()
            .map(|route| route.edges[0].to_sys_id().to_string())
            .collect::<Vec<_>>();
        branches.sort();
        assert_eq!(branches, vec![branch_a.to_string(), branch_b.to_string()]);
    }

    /// Task #10: the path-edge traversal endpoints are no longer stored — they are
    /// derived from `parent_sys_id`/`child_sys_id`/`direction`. This pins that the
    /// derivation matches the old stored semantics for BOTH crossing directions,
    /// and that a path's depth equals its edge count.
    #[test]
    fn business_application_path_edge_derives_endpoints_from_direction() {
        let parent = "pppppppppppppppppppppppppppppppp";
        let child = "cccccccccccccccccccccccccccccccc";
        let rel = BusinessApplicationRelationshipEdge {
            parent_sys_id: parent.to_string(),
            child_sys_id: child.to_string(),
            parent_class: None,
            child_class: None,
            relationship_type: BusinessApplicationRelationshipType {
                value: "dep".to_string(),
                display_value: None,
            },
        };

        // Parent→child crossing: traversal entered at the parent, exited at child.
        let down = rel.path_edge(1, BusinessApplicationRelationshipDirection::ParentToChild);
        assert_eq!(down.from_sys_id(), parent);
        assert_eq!(down.to_sys_id(), child);

        // Child→parent crossing: the endpoints flip.
        let up = rel.path_edge(2, BusinessApplicationRelationshipDirection::ChildToParent);
        assert_eq!(up.from_sys_id(), child);
        assert_eq!(up.to_sys_id(), parent);

        // A path's depth/len is exactly its edge count.
        let path = BusinessApplicationServerPath {
            edges: vec![down, up],
        };
        assert_eq!(path.depth(), 2);
        assert_eq!(path.len(), 2);
        assert!(!path.is_empty());
        assert!(BusinessApplicationServerPath { edges: vec![] }.is_empty());
    }

    /// Fix #5: the paginated edge read must surface truncation when the
    /// `max_edges` budget is consumed while more edge pages remain. Here
    /// `max_edges = 2` and the first (and only fetched) page returns exactly two
    /// edges, filling the page; with `no_count()` the paginator cannot prove it
    /// is done, so reaching the budget with the paginator not exhausted sets
    /// `edge_limit_reached`. This is the boundary the old single-request guard
    /// (`rows.len() > remaining`) could never detect when a server-side cap
    /// returned fewer rows than requested.
    #[tokio::test]
    async fn business_application_servers_edge_budget_paginates_and_truncates() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let ci_one = "22222222222222222222222222222222";
        let ci_two = "33333333333333333333333333333333";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // First parent page is full (2 == page size derived from max_edges=2),
        // so the paginator believes more pages may exist. The budget caps here.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .and(query_param("sysparm_offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, ci_one, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row("99999999999999999999999999999992", app, ci_two, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                max_edges: Some(2),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert!(
            result.relationship_summary.edge_limit_reached,
            "consuming max_edges with pages remaining must set edge_limit_reached"
        );
        assert!(result.relationship_summary.truncated);
        assert_eq!(result.relationship_summary.relationships_examined, 2);
    }

    /// Task #8 regression: the parent and child directions are read CONCURRENTLY
    /// but share a single `max_edges` budget that must be consumed in a stable
    /// parent-then-child order. Here `max_edges = 2`, the parent direction returns
    /// one edge and the child direction returns two. The merge must credit the
    /// parent's single edge first, then cap the child's contribution to the one
    /// remaining unit of budget — yielding exactly two examined edges (never three)
    /// and flagging truncation. This proves concurrency does not double-count or
    /// exceed the shared budget, and that the merge order is deterministic.
    #[tokio::test]
    async fn business_application_servers_shared_edge_budget_splits_across_directions() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let ci_one = "22222222222222222222222222222222";
        let ci_two = "33333333333333333333333333333333";
        let ci_three = "44444444444444444444444444444444";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Parent direction: a single edge. Merged first, it consumes one unit of
        // the shared budget (max_edges = 2).
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, ci_one, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .mount(&server)
            .await;

        // Child direction: two edges, but only one unit of budget is left after the
        // parent merge, so exactly one of these survives the merge truncation.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999992", ci_two, app, rel_type, "cmdb_ci_appl", "cmdb_ci_business_app"),
                    relationship_row("99999999999999999999999999999993", ci_three, app, rel_type, "cmdb_ci_appl", "cmdb_ci_business_app")
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                max_edges: Some(2),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.relationship_summary.relationships_examined, 2,
            "shared budget must cap combined parent+child edges at max_edges"
        );
        assert!(
            result.relationship_summary.edge_limit_reached,
            "truncating the child direction at the shared budget sets edge_limit_reached"
        );
        assert!(result.relationship_summary.truncated);
    }

    /// Fix #4: `max_cis` bounds the CIs examined BEYOND the root BA. With
    /// `max_cis = 1` and two adjacent CIs, exactly one non-root CI is examined
    /// and the second is truncated. The root BA does not consume the budget, so a
    /// caller asking for one CI is not silently short-changed to zero.
    #[tokio::test]
    async fn business_application_servers_reports_ci_limit_truncation() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let service_two = "55555555555555555555555555555555";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        // Two adjacent non-root CIs; with max_cis=1 only the first is examined.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(
                        "99999999999999999999999999999991",
                        app,
                        service,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    ),
                    relationship_row(
                        "99999999999999999999999999999992",
                        app,
                        service_two,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;
        // The first examined CI (service) is hydrated as a non-server class and
        // expanded into the next depth; it has no further relationships.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{service}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{service}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                max_cis: Some(1),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert!(result.servers.is_empty());
        assert!(result.relationship_summary.ci_limit_reached);
        assert!(result.relationship_summary.truncated);
        assert_eq!(result.relationship_summary.truncated_count, 1);
        // One non-root CI examined plus the root => 2 in cis_examined.
        assert_eq!(result.relationship_summary.cis_examined, 2);
        assert_eq!(
            result
                .relationship_summary
                .degraded_reasons
                .get("fanout_limit_exceeded")
                .copied(),
            Some(1)
        );
        // The SECOND CI (service_two) is the one truncated by the budget.
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason == ReferenceResolutionReason::FanoutLimitExceeded
                && diagnostic.reference_sys_id == service_two
        }));
    }

    /// Fix #1 regression guard: a `cmdb_ci_server` subclass that is NOT in the
    /// legacy `SERVER_TABLES` allowlist (here `cmdb_ci_esx_server`) must still be
    /// classified as a server and hydrated, not traversed through as an
    /// intermediate CI. The hydration query targets the base `cmdb_ci_server`
    /// table and must NOT pin `sys_class_name` to the allowlist, otherwise the
    /// subclass record would be filtered out server-side.
    #[tokio::test]
    async fn business_application_servers_collects_server_subclasses() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let esx = "66666666666666666666666666666666";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rel_row = "99999999999999999999999999999991";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // The BA depends on an ESX server subclass directly.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(
                        rel_row,
                        app,
                        esx,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_esx_server"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Hydration must query the base server table by sys_id only (no
        // sys_class_name allowlist filter) so the ESX subclass row is returned.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{esx}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(esx, "esx-alpha.example.com", "cmdb_ci_esx_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1, "ESX subclass should be a server");
        assert_eq!(result.servers[0].record.sys_id, esx);
        assert_eq!(
            result.servers[0].class_name.as_deref(),
            Some("cmdb_ci_esx_server")
        );
        assert_eq!(result.relationship_summary.servers_found, 1);
    }

    /// Task #1-deeper: a server subclass whose table name does NOT end in
    /// `_server` and is in no allowlist (here `cmdb_ci_acme_compute`) is invisible
    /// to every cheap heuristic. It must still be recognized as a server via the
    /// metadata-backed `sys_db_object` super_class descent — the class extends
    /// `cmdb_ci_server`, so walking its ancestry reveals the server base table and
    /// the CI is collected/hydrated rather than traversed through.
    #[tokio::test]
    async fn business_application_servers_detects_custom_subclass_via_hierarchy() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let compute = "77777777777777777777777777777777";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rel_row = "99999999999999999999999999999991";
        // sys_id of the cmdb_ci_server class row in sys_db_object.
        let server_class = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // The BA depends on a custom server subclass whose name does not end in
        // `_server`, so no cheap heuristic classifies it.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(
                        rel_row,
                        app,
                        compute,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_acme_compute"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;

        // super_class descent: cmdb_ci_acme_compute -> (super_class sys_id) ->
        // cmdb_ci_server, which terminates the walk.
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(query_param("sysparm_query", "name=cmdb_ci_acme_compute"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": "cmdb_ci_acme_compute", "super_class": server_class }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(query_param(
                "sysparm_query",
                format!("sys_id={server_class}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": "cmdb_ci_server" }]
            })))
            .mount(&server)
            .await;
        // Once cmdb_ci_server becomes the cursor, terminate the walk.
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(query_param("sysparm_query", "name=cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": "cmdb_ci_server", "super_class": "" }]
            })))
            .mount(&server)
            .await;

        // Hydration of the recognized server by sys_id against the base table.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{compute}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(compute, "acme-compute-01.example.com", "cmdb_ci_acme_compute")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.servers.len(),
            1,
            "custom subclass extending cmdb_ci_server must be detected via hierarchy"
        );
        assert_eq!(result.servers[0].record.sys_id, compute);
        assert_eq!(result.relationship_summary.servers_found, 1);
    }

    /// Fix #2 regression guard: with the default (unspecified) relationship-type
    /// filter, an edge must match by the stable `cmdb_rel_type` identity (sys_id)
    /// even when the instance's display label has been renamed/localized so it no
    /// longer equals any default label string. The traversal resolves the default
    /// label set to sys_ids once via a `cmdb_rel_type` lookup and matches on those.
    #[tokio::test]
    async fn business_application_servers_default_types_match_by_resolved_identity() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let linux = "33333333333333333333333333333333";
        // sys_id of the "Depends on::Used by" cmdb_rel_type on this instance.
        let depends_on = "dededededededededededededededede";
        let rel_row = "99999999999999999999999999999991";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // The default-label resolution query against cmdb_rel_type returns the
        // sys_id of the "Depends on::Used by" type (by its stored name).
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_type"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    { "sys_id": depends_on, "name": "Depends on::Used by" }
                ]
            })))
            .mount(&server)
            .await;

        // The edge carries the resolved sys_id but a RENAMED display label that
        // does not equal any default label string.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        rel_row,
                        app,
                        linux,
                        depends_on,
                        "Depende de::Usado por",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.servers.len(),
            1,
            "default filter must match renamed-label edge by resolved sys_id"
        );
        assert_eq!(result.servers[0].record.sys_id, linux);
    }

    /// Fix #2: an explicitly-supplied EMPTY relationship-type allowlist means
    /// "match all", so an edge with an arbitrary type is still traversed and its
    /// server collected, and no cmdb_rel_type resolution query is required.
    #[tokio::test]
    async fn business_application_servers_explicit_empty_types_match_all() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let linux = "33333333333333333333333333333333";
        let weird_type = "cccccccccccccccccccccccccccccccc";
        let rel_row = "99999999999999999999999999999991";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // An arbitrary, non-default relationship type with an unfamiliar label.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        rel_row,
                        app,
                        linux,
                        weird_type,
                        "Some Custom Relationship",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        // An explicit empty allowlist (caller passed `relationship_type: []`
        // explicitly via the options) means "match all".
        let options = BusinessApplicationServersOptions {
            selector: BusinessApplicationServersSelector::Number("APM0000001".to_string()),
            max_depth: 2,
            max_cis: 500,
            max_edges: 2000,
            max_service_membership_associations:
                BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
            max_service_membership_pages:
                BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
            relationship_type: Vec::new(),
            include_paths: false,
            fallback_strategy: FallbackStrategy::None,
            persist: false,
            prune_stale: false,
        };
        // defaults_when_empty = false => empty allowlist means "match all".
        let result = core
            .business_applications
            .business_application_servers_with_options(options, false)
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.servers.len(),
            1,
            "explicit empty allowlist must match all relationship types"
        );
    }

    fn service_membership_row(
        sys_id: &str,
        service_id: &str,
        ci_id: &str,
        ci_class_name: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "sys_id": sys_id,
            "service_id": {
                "value": service_id,
                "display_value": "Application Service"
            },
            "service_id.sys_class_name": "cmdb_ci_service",
            "ci_id": {
                "value": ci_id,
                "display_value": "Server CI"
            },
            "ci_id.sys_class_name": ci_class_name
        })
    }

    #[tokio::test]
    async fn business_application_servers_returns_service_membership_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [service_membership_row(
                    "88888888888888888888888888888881",
                    service,
                    linux,
                    "cmdb_ci_linux_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-service.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        assert_eq!(
            result.server_provenance.get(linux),
            Some(&BusinessApplicationServerProvenance::ServiceMembership)
        );
        let path = &result.server_paths[linux][0];
        assert_eq!(path.depth(), 2);
        assert_eq!(
            path.edges.last().map(|edge| &edge.edge_source),
            Some(&BusinessApplicationServerPathEdgeSource::ServiceMembership)
        );
        assert_eq!(
            path.edges
                .last()
                .map(|edge| edge.relationship_type.value.as_str()),
            Some("service_member_of")
        );
        assert_eq!(
            result
                .inventory_health
                .as_ref()
                .map(|health| health.service_membership_status.as_str()),
            Some("ok")
        );

        let cached = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(app.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward lookup")
            .expect("business application cached");
        assert_eq!(cached.servers.len(), 1);
        assert_eq!(cached.servers[0].provenance, "service_membership");
        assert_eq!(cached.servers[0].min_depth, 2);
    }

    #[tokio::test]
    async fn business_application_servers_merges_relationship_and_service_membership_provenance() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        linux,
                        runs,
                        "Runs on::Runs",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    ),
                    relationship_row_typed(
                        "99999999999999999999999999999992",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [service_membership_row(
                    "88888888888888888888888888888881",
                    service,
                    linux,
                    "cmdb_ci_linux_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-both.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(2)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        assert_eq!(
            result.server_provenance.get(linux),
            Some(&BusinessApplicationServerProvenance::Both)
        );
        assert_eq!(result.server_paths[linux].len(), 2);

        let cached = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(app.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward lookup")
            .expect("business application cached");
        assert_eq!(cached.servers.len(), 1);
        assert_eq!(cached.servers[0].provenance, "both");
    }

    #[tokio::test]
    async fn business_application_servers_service_membership_acl_degrades_relationship_results() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        linux,
                        runs,
                        "Runs on::Runs",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    ),
                    relationship_row_typed(
                        "99999999999999999999999999999992",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "message": "Forbidden" },
                "status": "failure"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-acl.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        let health = result.inventory_health.expect("inventory health");
        assert_eq!(health.service_membership_status, "acl_restricted");
        assert_eq!(health.relationship_status, "ok");
        assert_eq!(health.inventory_status, "service_membership_degraded");
    }

    #[tokio::test]
    async fn business_application_servers_service_membership_accepts_server_subclasses() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let esx = "66666666666666666666666666666666";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [service_membership_row(
                    "88888888888888888888888888888881",
                    service,
                    esx,
                    "cmdb_ci_esx_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{esx}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(esx, "esx-service.example.com", "cmdb_ci_esx_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        assert_eq!(result.servers[0].record.sys_id, esx);
        assert_eq!(
            result.server_provenance.get(esx),
            Some(&BusinessApplicationServerProvenance::ServiceMembership)
        );
    }

    // ---- ci_owner_group CMDB-gap fallback fixtures (Part 2) ----------------

    const FALLBACK_APP: &str = "11111111111111111111111111111111";
    const FALLBACK_GROUP: &str = "9999999999999999999999999999aaaa";
    const FALLBACK_LINUX: &str = "33333333333333333333333333333333";
    const FALLBACK_WINDOWS: &str = "44444444444444444444444444444444";

    /// Mount a BA record with the RAW `u_ci_owner_group` field populated and a
    /// present-but-empty `managed_by_group` alias — the live-data shape the
    /// field-mapping requirement guards against.
    async fn mount_fallback_ba(server: &MockServer, group_sys_id: Option<&str>) {
        let mut record = serde_json::json!({
            "sys_id": FALLBACK_APP,
            "number": "APM0000001",
            "name": "Application Alpha",
            "sys_class_name": "cmdb_ci_business_app",
            // The typed `ci_owner_group` alias maps here and is intentionally
            // empty on live data; the fallback must NOT read it.
            "managed_by_group": { "value": "", "display_value": "" }
        });
        if let Some(group_sys_id) = group_sys_id {
            record.as_object_mut().unwrap().insert(
                BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD.to_string(),
                serde_json::json!({ "value": group_sys_id, "display_value": "Owner Group" }),
            );
        }
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [record]
            })))
            .mount(server)
            .await;
    }

    /// Mount the two empty `cmdb_rel_ci` direction reads so the traversal finds 0
    /// servers (default depth 2 only issues the depth-1 frontier reads).
    async fn mount_empty_traversal(server: &MockServer) {
        for direction in ["parent", "child"] {
            Mock::given(method("GET"))
                .and(path("/api/now/table/cmdb_rel_ci"))
                .and(query_param(
                    "sysparm_query",
                    format!("{direction}IN{FALLBACK_APP}"),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "result": []
                })))
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_returns_tagged_servers_when_traversal_empty() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server"),
                    server_row(FALLBACK_WINDOWS, "windows-fallback.example.com", "cmdb_ci_win_server")
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert_eq!(result.servers.len(), 2);
        for server in &result.servers {
            assert_eq!(
                result.server_sources.get(&server.record.sys_id),
                Some(&ServerResultSource::CiOwnerGroupFallback)
            );
        }
        let summary = &result.relationship_summary;
        assert!(summary.fallback_used);
        assert_eq!(summary.cmdb_servers_found, Some(0));
        assert_eq!(summary.servers_found, 2);
        assert_eq!(summary.fallback_strategy.as_deref(), Some("ci_owner_group"));
        assert_eq!(
            summary.fallback_group_sys_id.as_deref(),
            Some(FALLBACK_GROUP)
        );
        assert_eq!(
            summary.fallback_group_display_name.as_deref(),
            Some("Owner Group")
        );
        assert_eq!(
            summary
                .degraded_reasons
                .get(BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED),
            Some(&1)
        );
        // Fallback never persists: no traversal servers, no membership upserts.
        assert_eq!(summary.persisted_servers, 0);
        assert_eq!(summary.membership_upserts, 0);
        assert!(result.server_provenance.is_empty());
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_fires_even_though_managed_by_group_empty() {
        let _guard = mock_server_test_lock().await;
        // Field-mapping proof: the BA's managed_by_group alias is empty; the
        // fallback fires because it sources/filters on the RAW u_ci_owner_group.
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        // The ONLY server query the fallback may issue is the exact raw-field
        // filter. A managed_by_group-based query would not match this mock and
        // the call would 404/timeout, failing the test.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert_eq!(result.servers.len(), 1);
        assert!(result.relationship_summary.fallback_used);
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_does_not_fire_when_traversal_finds_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{FALLBACK_APP}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [relationship_row(
                    "99999999999999999999999999999991",
                    FALLBACK_APP,
                    FALLBACK_LINUX,
                    rel_type,
                    "cmdb_ci_business_app",
                    "cmdb_ci_linux_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("childIN{FALLBACK_APP}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{FALLBACK_LINUX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-real.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert_eq!(result.servers.len(), 1);
        // Fallback did NOT fire: no fallback servers, no degraded gap reason.
        assert!(!result.relationship_summary.fallback_used);
        assert!(result.server_sources.is_empty());
        // cmdb_servers_found still reported (strategy requested) and equals the
        // traversal count.
        assert_eq!(result.relationship_summary.cmdb_servers_found, Some(1));
        assert_eq!(
            result.server_sources.get(FALLBACK_LINUX),
            None,
            "traversal servers are not tagged as fallback"
        );
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_no_group_emits_clean_diagnostic() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, None).await;
        mount_empty_traversal(&server).await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(!result.relationship_summary.fallback_used);
        assert_eq!(result.relationship_summary.cmdb_servers_found, Some(0));
        // Clean structured diagnostic, not an error.
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field == BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD
        }));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_strategy_none_adds_no_fields() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                // default: FallbackStrategy::None
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(result.server_sources.is_empty());
        let summary = &result.relationship_summary;
        assert!(!summary.fallback_used);
        assert_eq!(summary.cmdb_servers_found, None);
        assert_eq!(summary.fallback_strategy, None);
        assert_eq!(summary.fallback_group_sys_id, None);
        // No new fields appear in the default-path serialization.
        let serialized = serde_json::to_value(summary).expect("serialize summary");
        let object = serialized.as_object().expect("summary object");
        assert!(!object.contains_key("cmdb_servers_found"));
        assert!(!object.contains_key("fallback_used"));
        assert!(!object.contains_key("fallback_strategy"));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_group_with_zero_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!(
                    "{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"
                ),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        // The owner group exists and was queried, so the data-quality gap is real:
        // fallback_used is true even though it returned no servers.
        assert!(result.relationship_summary.fallback_used);
        assert_eq!(result.relationship_summary.cmdb_servers_found, Some(0));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_acl_restricted_query() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!(
                    "{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"
                ),
            ))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "message": "ACL restricted" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(!result.relationship_summary.fallback_used);
        assert!(result.relationship_summary.acl_restricted_count >= 1);
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason == ReferenceResolutionReason::ReferenceAclRestricted
        }));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_tombstoned_group_no_panic() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!(
                    "{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"
                ),
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": { "message": "No record found" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(!result.relationship_summary.fallback_used);
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field == BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD
                && diagnostic.reason == ReferenceResolutionReason::ReferenceNotFound
        }));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_writes_no_durable_membership_rows() {
        let _guard = mock_server_test_lock().await;
        // Load-bearing live-only assertion: a fallback-triggering run leaves the
        // durable BA↔server inventory tables unchanged. We run with persist=true
        // (the CLI/daemon default) so the only writes would come from the
        // traversal path; the fallback servers must not appear in the cached
        // forward/reverse projections or the inventory-health row.
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        // The live response carries the fallback server...
        assert_eq!(result.servers.len(), 1);
        assert!(result.relationship_summary.fallback_used);
        // ...but nothing was persisted: 0 membership upserts, 0 persisted servers.
        assert_eq!(result.relationship_summary.persisted_servers, 0);
        assert_eq!(result.relationship_summary.membership_upserts, 0);

        // The durable forward projection has NO servers for this BA.
        let cached_forward = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(FALLBACK_APP.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward");
        if let Some(cached) = cached_forward {
            assert!(
                cached.servers.is_empty(),
                "fallback server must not be persisted to the forward projection"
            );
        }

        // The durable reverse projection has NO BA for the fallback server.
        let cached_reverse = core
            .business_applications_for_server(BusinessApplicationsForServerParams {
                sys_id: Some(FALLBACK_LINUX.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached reverse");
        if let Some(cached) = cached_reverse {
            for entry in &cached.servers {
                assert!(
                    entry.business_applications.is_empty(),
                    "fallback server must not gain a durable BA association"
                );
            }
        }
    }

    fn relationship_row(
        sys_id: &str,
        parent: &str,
        child: &str,
        relationship_type: &str,
        parent_class: &str,
        child_class: &str,
    ) -> serde_json::Value {
        relationship_row_typed(
            sys_id,
            parent,
            child,
            relationship_type,
            "Depends on::Used by",
            parent_class,
            child_class,
        )
    }

    /// Like [`relationship_row`] but lets a test set the relationship type's
    /// display label independently of its sys_id, so Fix #2 can simulate a
    /// renamed/localized `cmdb_rel_type` label.
    fn relationship_row_typed(
        sys_id: &str,
        parent: &str,
        child: &str,
        relationship_type: &str,
        relationship_type_label: &str,
        parent_class: &str,
        child_class: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "sys_id": sys_id,
            "parent": { "value": parent, "display_value": "Parent CI" },
            "child": { "value": child, "display_value": "Child CI" },
            "type": {
                "value": relationship_type,
                "display_value": relationship_type_label
            },
            "parent.sys_class_name": parent_class,
            "child.sys_class_name": child_class
        })
    }

    fn server_row(sys_id: &str, name: &str, class_name: &str) -> serde_json::Value {
        serde_json::json!({
            "sys_id": sys_id,
            "name": name,
            "sys_class_name": class_name,
            "ip_address": "192.0.2.10",
            "operational_status": { "value": "1", "display_value": "Operational" }
        })
    }

    /// Mount a `sys_db_object` response so `table_ancestors` terminates with no
    /// parent (the Business Application table is treated as its own root).
    async fn mount_no_table_ancestors(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": BUSINESS_APPLICATION_TABLE, "super_class": "" }]
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn business_application_dictionary_refresh_caches_metadata_and_promotes_aliases() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_dictionary"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "name": BUSINESS_APPLICATION_TABLE,
                        "element": "portfolio",
                        "column_label": "Primary Portfolio",
                        "internal_type": { "value": "reference", "display_value": "Reference" },
                        "reference": { "value": "pm_portfolio", "display_value": "Portfolio" },
                        "choice": "0",
                        "mandatory": "false",
                        "read_only": "false",
                        "max_length": "32",
                        "active": "true"
                    },
                    {
                        "name": BUSINESS_APPLICATION_TABLE,
                        "element": "operational_status",
                        "column_label": "Operational State",
                        "internal_type": { "value": "choice", "display_value": "Choice" },
                        "reference": "",
                        "choice": "1",
                        "mandatory": "false",
                        "read_only": "false",
                        "max_length": "40",
                        "active": "true"
                    }
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let persisted = core
            .refresh_business_application_dictionary()
            .await
            .expect("dictionary refresh");
        assert_eq!(persisted, 2);

        let dictionary = core
            .business_application_dictionary()
            .await
            .expect("dictionary read");
        assert_eq!(
            dictionary
                .get("portfolio")
                .and_then(|row| row.reference_table.clone()),
            Some("pm_portfolio".to_string())
        );
        assert_eq!(
            dictionary
                .get("operational_status")
                .map(|row| row.field_type.clone()),
            Some(Some("choice".to_string()))
        );
        assert!(dictionary["operational_status"].choice);

        let aliases = core.business_application_aliases().await.expect("aliases");
        // Dictionary-verified: portfolio target table discovered, version set,
        // and no DictionaryUnavailable diagnostic.
        assert_eq!(aliases.primary_portfolio, "portfolio");
        assert_eq!(
            aliases.primary_portfolio_table,
            Some("pm_portfolio".to_string())
        );
        assert!(aliases.dictionary_version.is_some());
        assert!(aliases.diagnostics.is_empty());
    }

    #[tokio::test]
    async fn business_application_aliases_degrade_without_dictionary() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        let (core, _tempdir) = core_for_mock_server(&server).await;
        let aliases = core.business_application_aliases().await.expect("aliases");
        // Cache miss => baseline degraded with a DictionaryUnavailable diagnostic.
        assert_eq!(aliases.primary_portfolio, "portfolio");
        assert!(aliases.dictionary_version.is_none());
        assert!(aliases.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason == ReferenceResolutionReason::DictionaryUnavailable
        }));
    }

    #[tokio::test]
    async fn business_application_sync_summarizes_run() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": { "value": owner_sys_id, "display_value": "Jane Owner" },
                    "portfolio": { "value": "portfolio-sys-id-000000000000000", "display_value": "Clinical" },
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sys_user/{owner_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": owner_sys_id,
                    "name": "Jane Owner",
                    "user_name": "jowner",
                    "email": "jane.owner@example.invalid",
                    "active": { "value": "true", "display_value": "true" },
                    "sys_updated_on": "2026-05-30 12:00:00"
                }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let summary = core
            .sync_business_applications(
                Some(BusinessApplicationSearchParams {
                    name: Some("Epic".to_string()),
                    limit: Some(5),
                    ..Default::default()
                }),
                BusinessApplicationHydrationOptions::default(),
            )
            .await
            .expect("sync summary");

        assert!(!summary.all);
        assert_eq!(summary.table, BUSINESS_APPLICATION_TABLE);
        assert_eq!(summary.page_size, 5);
        assert_eq!(summary.pages, 1);
        assert_eq!(summary.total_returned, 1);
        assert_eq!(summary.total_applications, 1);
        assert_eq!(summary.persisted, 1);
        // Owner reference resolves; the portfolio reference is degraded because
        // no dictionary was loaded for this run.
        assert!(summary.references_resolved >= 1);
        assert!(summary.dictionary_degraded);
        assert!(!summary.dictionary_refreshed);
        assert!(
            summary
                .degraded_reasons
                .contains_key("dictionary_unavailable")
        );
    }

    #[tokio::test]
    async fn business_application_sync_non_persistent_reports_zero_persisted() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let summary = core
            .sync_business_applications(
                None,
                BusinessApplicationHydrationOptions {
                    persist: false,
                    ..Default::default()
                },
            )
            .await
            .expect("sync summary");

        assert_eq!(summary.total_applications, 1);
        assert_eq!(summary.total_returned, 1);
        assert_eq!(summary.persisted, 0);
    }

    fn business_application_page_record(index: usize) -> serde_json::Value {
        serde_json::json!({
            "sys_id": format!("{:032x}", index + 1),
            "name": format!("Application {:03}", index + 1),
            "number": format!("APP{:07}", index + 1),
            "sys_class_name": BUSINESS_APPLICATION_TABLE,
            "operational_status": { "value": "1", "display_value": "Operational" }
        })
    }

    #[tokio::test]
    async fn business_application_sync_all_drains_live_pages_and_persists_each_page() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        let first_page = (0..100)
            .map(business_application_page_record)
            .collect::<Vec<_>>();
        let second_page = vec![business_application_page_record(100)];

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param("sysparm_limit", "100"))
            .and(query_param("sysparm_offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": first_page
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param("sysparm_limit", "100"))
            .and(query_param("sysparm_offset", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": second_page
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let summary = core
            .sync_all_business_applications(BusinessApplicationHydrationOptions::default())
            .await
            .expect("sync all summary");

        assert!(summary.all);
        assert_eq!(summary.table, BUSINESS_APPLICATION_TABLE);
        assert_eq!(summary.page_size, 100);
        assert_eq!(summary.pages, 2);
        assert_eq!(summary.total_returned, 101);
        assert_eq!(summary.total_applications, 101);
        assert_eq!(summary.persisted, 101);
        assert!(summary.dictionary_degraded);
        assert_eq!(
            summary
                .degraded_reasons
                .get("dictionary_unavailable")
                .copied(),
            Some(101)
        );

        let cached = core
            .query_business_applications(BusinessApplicationQuery {
                limit: Some(200),
                ..Default::default()
            })
            .await
            .expect("cached business applications");
        assert_eq!(cached.len(), 101);

        let requests = server.received_requests().await.expect("requests");
        let business_app_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/cmdb_ci_business_app")
            .collect::<Vec<_>>();
        assert_eq!(business_app_requests.len(), 2);
        for request in business_app_requests {
            let query = request
                .url
                .query_pairs()
                .collect::<std::collections::HashMap<_, _>>();
            assert_eq!(
                query.get("sysparm_query").map(|value| value.as_ref()),
                Some("sys_class_name=cmdb_ci_business_app^ORDERBYname^ORDERBYsys_id")
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
        }
    }

    #[test]
    fn business_application_reference_discovery_uses_known_map() {
        let record = Record::from_json(
            BUSINESS_APPLICATION_TABLE,
            &serde_json::json!({
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "name": "Epic",
                "business_owner": {
                    "value": "6816f79cc0a8016401c5a33be04be441",
                    "display_value": "Jane Owner"
                },
                "support_group": {
                    "value": "287ebd7da9fe198100f92cc8d1d2154e",
                    "display_value": "App Support"
                },
                "portfolio": {
                    "value": "46d44a23a9fe19810012d100cca80666",
                    "display_value": "Clinical"
                }
            }),
            DisplayValue::Both,
        )
        .expect("record");

        let business_application = BusinessApplication::from_servicenow(
            &record,
            &BusinessApplicationFieldAliases::baseline_degraded(),
        )
        .expect("business application");

        assert_eq!(
            business_application
                .business_owner
                .as_ref()
                .map(|reference| reference.table.as_str()),
            Some("sys_user")
        );
        assert_eq!(
            business_application
                .primary_support_group
                .as_ref()
                .map(|reference| reference.table.as_str()),
            Some("sys_user_group")
        );
        assert!(
            business_application
                .references
                .iter()
                .any(|reference| reference.field == "portfolio"
                    && reference.reference_table == "pm_portfolio"
                    && reference.resolution_status == ReferenceResolutionStatus::Resolved)
        );
    }
}
