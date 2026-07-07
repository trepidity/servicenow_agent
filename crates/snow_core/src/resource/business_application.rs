use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use servicenow_rs::prelude::Record;

use super::server::Server;
use crate::helpers::non_empty_owned;
use crate::{
    CacheSource, FieldValue, Reference, SnowRecord, choose_reference_display_name,
    normalize_record_lookup_sys_id,
};

pub const BUSINESS_APPLICATION_TABLE: &str = "cmdb_ci_business_app";
pub const BUSINESS_APPLICATION_RESOURCE_TYPE: &str = "business_application";
/// Raw CMDB column holding the CI owner group on both `cmdb_ci_business_app`
/// records and `cmdb_ci_server` records on this instance.
///
/// IMPORTANT: this is intentionally NOT the `ci_owner_group` typed alias. The
/// typed alias resolves to `managed_by_group` (see
/// [`BusinessApplicationFieldAliases::baseline`] and the server reference map in
/// `resource/server.rs`), which is present-but-empty on live data. The
/// `ci_owner_group` CMDB-gap fallback must source and filter on this raw column
/// directly so it does not silently never fire. The same field name is used on
/// both the BA and the server side and targets `sys_user_group`, so the group
/// sys_id matches across the two reads without any translation.
pub const BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD: &str = "u_ci_owner_group";
/// `relationship_summary.degraded_reasons` key set when the `ci_owner_group`
/// fallback fires, surfacing the CMDB data-quality gap (servers exist by owner
/// group but are not linked to the BA via `cmdb_rel_ci`).
pub const BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED: &str =
    "cmdb_relationships_unmapped";
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH: usize = 2;
pub const BUSINESS_APPLICATION_SERVERS_MAX_DEPTH: usize = 4;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS: usize = 500;
pub const BUSINESS_APPLICATION_SERVERS_MAX_CIS: usize = 5000;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES: usize = 2000;
pub const BUSINESS_APPLICATION_SERVERS_MAX_EDGES: usize = 20000;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS: usize = 2000;
pub const BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS: usize = 20000;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES: usize = 20;
pub const BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_PAGES: usize = 200;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES: &[&str] = &[
    "Depends on::Used by",
    "Runs on::Runs",
    "Contains::Contained by",
    "Hosted on::Hosts",
    "Instantiates::Instantiated by",
    "Members::Member of",
];
pub const BUSINESS_APPLICATION_SERVICE_DISCOVERY_RELATIONSHIP_TYPES: &[&str] =
    &["Consumes::Consumed by", "Uses::Used by"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplication {
    pub record: SnowRecord,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub business_owner: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_owner: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_support_group: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_state: Option<ChoiceValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_portfolio: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attested_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_dictionary_version: Option<String>,
    #[serde(default)]
    pub fields: BTreeMap<String, BusinessApplicationFieldValue>,
    #[serde(default)]
    pub references: Vec<ReferencePrimitiveDescriptor>,
    #[serde(default)]
    pub unresolved_references: Vec<ReferenceResolutionDiagnostic>,
}

/// Strategy applied when the `cmdb_rel_ci` traversal returns zero servers.
///
/// The fallback is opt-in and only-on-empty: it never fires when the traversal
/// finds one or more servers, regardless of this value.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FallbackStrategy {
    /// No fallback. Exact current behavior; a 0-server traversal returns
    /// `servers: []` with no new response fields.
    #[default]
    None,
    /// When the traversal finds 0 servers, query `cmdb_ci_server` by the BA's
    /// raw `u_ci_owner_group` field (NOT the empty `managed_by_group` alias) and
    /// return the matched servers tagged `ci_owner_group_fallback`, live-only.
    CiOwnerGroup,
}

impl FallbackStrategy {
    /// True when this strategy requests a fallback (anything but `None`).
    pub fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Stable wire/string identity used in the `relationship_summary`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CiOwnerGroup => "ci_owner_group",
        }
    }
}

/// Per-server provenance tag distinguishing CMDB traversal results from
/// `ci_owner_group` fallback results in the response.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ServerResultSource {
    /// Server discovered via the authoritative `cmdb_rel_ci` graph (or service
    /// membership). This is the default; surfaces omit the `source` field for it.
    #[default]
    CmdbRelCi,
    /// Server discovered via the read-time `u_ci_owner_group` fallback. Live-only;
    /// never persisted to the durable BA↔server inventory.
    CiOwnerGroupFallback,
}

impl ServerResultSource {
    /// Stable wire/string identity used in the per-server `source` field.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CmdbRelCi => "cmdb_rel_ci",
            Self::CiOwnerGroupFallback => "ci_owner_group_fallback",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct BusinessApplicationServersParams {
    /// Real Business Application number, for example <APM_NUMBER>.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number: Option<String>,
    /// Business Application sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sys_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_depth: Option<usize>,
    /// Maximum number of configuration items EXAMINED BEYOND the root Business
    /// Application during traversal. The root BA itself is never counted against
    /// this budget, so a caller passing `max_cis = N` may examine up to `N`
    /// non-root CIs. Defaults to
    /// [`BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cis: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_edges: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_service_membership_associations: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_service_membership_pages: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relationship_type: Vec<String>,
    #[serde(default)]
    pub include_paths: bool,
    /// Opt-in fallback used ONLY when the `cmdb_rel_ci` traversal returns zero
    /// servers. Defaults to [`FallbackStrategy::None`], which preserves the exact
    /// current behavior (no fallback, no new response fields). See
    /// [`FallbackStrategy`] for the CMDB data-quality fallback.
    #[serde(default)]
    pub fallback_strategy: FallbackStrategy,
    /// Persist traversal results to the local cache/vault and BA/server
    /// membership table. Core defaults this to false so read-only callers must
    /// opt in explicitly; CLI/daemon surfaces can choose their own default.
    ///
    /// NOTE: persistence covers ONLY `cmdb_rel_ci`/service-membership traversal
    /// results. `ci_owner_group` fallback servers are live-only and are never
    /// written to the durable BA↔server membership or inventory-health tables
    /// regardless of this flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist: Option<bool>,
    /// Tombstone this BA's previously persisted membership rows that were not
    /// re-seen by this persisting traversal.
    #[serde(default)]
    pub prune_stale: bool,
}

impl BusinessApplicationServersParams {
    pub fn validate(&self) -> Result<BusinessApplicationServersOptions> {
        let number = non_empty_owned(self.number.as_deref());
        let sys_id = non_empty_owned(self.sys_id.as_deref());
        let persist = self.persist.unwrap_or(false);
        if self.prune_stale && !persist {
            anyhow::bail!("`prune_stale` requires `persist=true`");
        }
        match (number, sys_id) {
            (Some(_), Some(_)) => {
                anyhow::bail!("provide exactly one of `number` or `sys_id`")
            }
            (None, None) => anyhow::bail!("provide exactly one of `number` or `sys_id`"),
            (Some(number), None) => {
                let number = normalize_business_application_number(&number)?;
                Ok(BusinessApplicationServersOptions {
                    selector: BusinessApplicationServersSelector::Number(number),
                    max_depth: validate_bound(
                        "max_depth",
                        self.max_depth,
                        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH,
                        BUSINESS_APPLICATION_SERVERS_MAX_DEPTH,
                    )?,
                    max_cis: validate_bound(
                        "max_cis",
                        self.max_cis,
                        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS,
                        BUSINESS_APPLICATION_SERVERS_MAX_CIS,
                    )?,
                    max_edges: validate_bound(
                        "max_edges",
                        self.max_edges,
                        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES,
                        BUSINESS_APPLICATION_SERVERS_MAX_EDGES,
                    )?,
                    max_service_membership_associations: validate_bound(
                        "max_service_membership_associations",
                        self.max_service_membership_associations,
                        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
                        BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
                    )?,
                    max_service_membership_pages: validate_bound(
                        "max_service_membership_pages",
                        self.max_service_membership_pages,
                        BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
                        BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_PAGES,
                    )?,
                    relationship_type: normalized_relationship_types(&self.relationship_type),
                    include_paths: self.include_paths,
                    fallback_strategy: self.fallback_strategy,
                    persist,
                    prune_stale: self.prune_stale,
                })
            }
            (None, Some(sys_id)) => Ok(BusinessApplicationServersOptions {
                selector: BusinessApplicationServersSelector::SysId(
                    normalize_record_lookup_sys_id(&sys_id)?,
                ),
                max_depth: validate_bound(
                    "max_depth",
                    self.max_depth,
                    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH,
                    BUSINESS_APPLICATION_SERVERS_MAX_DEPTH,
                )?,
                max_cis: validate_bound(
                    "max_cis",
                    self.max_cis,
                    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS,
                    BUSINESS_APPLICATION_SERVERS_MAX_CIS,
                )?,
                max_edges: validate_bound(
                    "max_edges",
                    self.max_edges,
                    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES,
                    BUSINESS_APPLICATION_SERVERS_MAX_EDGES,
                )?,
                max_service_membership_associations: validate_bound(
                    "max_service_membership_associations",
                    self.max_service_membership_associations,
                    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
                    BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
                )?,
                max_service_membership_pages: validate_bound(
                    "max_service_membership_pages",
                    self.max_service_membership_pages,
                    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
                    BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_PAGES,
                )?,
                relationship_type: normalized_relationship_types(&self.relationship_type),
                include_paths: self.include_paths,
                fallback_strategy: self.fallback_strategy,
                persist,
                prune_stale: self.prune_stale,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BusinessApplicationServersSelector {
    Number(String),
    SysId(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServersOptions {
    pub selector: BusinessApplicationServersSelector,
    pub max_depth: usize,
    /// Maximum number of configuration items examined BEYOND the root Business
    /// Application. The root BA is excluded from this budget; up to `max_cis`
    /// non-root CIs may be examined before traversal truncates.
    pub max_cis: usize,
    pub max_edges: usize,
    pub max_service_membership_associations: usize,
    pub max_service_membership_pages: usize,
    #[serde(default)]
    pub relationship_type: Vec<String>,
    #[serde(default)]
    pub include_paths: bool,
    #[serde(default)]
    pub fallback_strategy: FallbackStrategy,
    #[serde(default)]
    pub persist: bool,
    #[serde(default)]
    pub prune_stale: bool,
}

impl BusinessApplicationServersOptions {
    pub fn effective_relationship_types(&self) -> Vec<String> {
        if self.relationship_type.is_empty() {
            BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES
                .iter()
                .map(|value| (*value).to_string())
                .collect()
        } else {
            self.relationship_type.clone()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServersResult {
    pub business_application: BusinessApplicationServerApplication,
    #[serde(default)]
    pub servers: Vec<Server>,
    /// Per-server provenance tag, keyed by server `sys_id`. Only populated for
    /// `ci_owner_group` fallback servers (tagged
    /// [`ServerResultSource::CiOwnerGroupFallback`]); traversal servers are the
    /// default [`ServerResultSource::CmdbRelCi`] and are omitted here so the
    /// default (no-fallback) response shape is unchanged. Surfaces use this map
    /// to attach the per-server `source` field.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub server_sources: BTreeMap<String, ServerResultSource>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub server_provenance: BTreeMap<String, BusinessApplicationServerProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_health: Option<BusinessApplicationServerInventoryHealth>,
    pub relationship_summary: BusinessApplicationServersSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ReferenceResolutionDiagnostic>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub server_paths: BTreeMap<String, Vec<BusinessApplicationServerPath>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct BusinessApplicationServersCachedParams {
    /// Business Application number, for example <APM_NUMBER>.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number: Option<String>,
    /// Business Application sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sys_id: Option<String>,
    /// Exact Business Application name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub include_tombstoned: bool,
}

impl BusinessApplicationServersCachedParams {
    pub fn validate(&self) -> Result<BusinessApplicationServersCachedOptions> {
        let number = non_empty_owned(self.number.as_deref());
        let sys_id = non_empty_owned(self.sys_id.as_deref());
        let name = non_empty_owned(self.name.as_deref());
        let selectors = [number.is_some(), sys_id.is_some(), name.is_some()]
            .into_iter()
            .filter(|selected| *selected)
            .count();
        if selectors != 1 {
            anyhow::bail!("provide exactly one of `number`, `sys_id`, or `name`");
        }
        let selector = if let Some(number) = number {
            BusinessApplicationServersCachedSelector::Number(normalize_business_application_number(
                &number,
            )?)
        } else if let Some(sys_id) = sys_id {
            BusinessApplicationServersCachedSelector::SysId(normalize_record_lookup_sys_id(
                &sys_id,
            )?)
        } else {
            BusinessApplicationServersCachedSelector::ExactName(
                name.expect("selector count already checked"),
            )
        };
        Ok(BusinessApplicationServersCachedOptions {
            selector,
            include_tombstoned: self.include_tombstoned,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BusinessApplicationServersCachedSelector {
    Number(String),
    SysId(String),
    ExactName(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServersCachedOptions {
    pub selector: BusinessApplicationServersCachedSelector,
    #[serde(default)]
    pub include_tombstoned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct BusinessApplicationsForServerParams {
    /// Server sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sys_id: Option<String>,
    /// Exact Server name. This may match more than one cached server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Exact Server IP address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(default)]
    pub include_tombstoned: bool,
}

impl BusinessApplicationsForServerParams {
    pub fn validate(&self) -> Result<BusinessApplicationsForServerOptions> {
        let sys_id = non_empty_owned(self.sys_id.as_deref());
        let name = non_empty_owned(self.name.as_deref());
        let ip_address = non_empty_owned(self.ip_address.as_deref());
        let selectors = [sys_id.is_some(), name.is_some(), ip_address.is_some()]
            .into_iter()
            .filter(|selected| *selected)
            .count();
        if selectors != 1 {
            anyhow::bail!("provide exactly one of `sys_id`, `name`, or `ip_address`");
        }
        let selector = if let Some(sys_id) = sys_id {
            BusinessApplicationsForServerSelector::SysId(normalize_record_lookup_sys_id(&sys_id)?)
        } else if let Some(name) = name {
            BusinessApplicationsForServerSelector::ExactName(name)
        } else {
            BusinessApplicationsForServerSelector::IpAddress(
                ip_address.expect("selector count already checked"),
            )
        };
        Ok(BusinessApplicationsForServerOptions {
            selector,
            include_tombstoned: self.include_tombstoned,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BusinessApplicationsForServerSelector {
    SysId(String),
    ExactName(String),
    IpAddress(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationsForServerOptions {
    pub selector: BusinessApplicationsForServerSelector,
    #[serde(default)]
    pub include_tombstoned: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServersCachedResult {
    pub business_application: SnowRecord,
    #[serde(default)]
    pub servers: Vec<CachedBusinessApplicationServer>,
    #[serde(default)]
    pub endpoint_status: EndpointResolutionStatus,
    #[serde(default)]
    pub relationship_status: RelationshipKnowledgeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_health: Option<BusinessApplicationServerInventoryHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedBusinessApplicationServer {
    pub server: SnowRecord,
    pub server_table: String,
    pub provenance: String,
    pub min_depth: usize,
    #[serde(default)]
    pub paths: Vec<BusinessApplicationServerPath>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationsForServerResult {
    #[serde(default)]
    pub servers: Vec<CachedServerBusinessApplications>,
    #[serde(default)]
    pub endpoint_status: EndpointResolutionStatus,
    #[serde(default)]
    pub relationship_status: RelationshipKnowledgeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedServerBusinessApplications {
    pub server: SnowRecord,
    #[serde(default)]
    pub business_applications: Vec<CachedBusinessApplicationForServer>,
    #[serde(default)]
    pub endpoint_status: EndpointResolutionStatus,
    #[serde(default)]
    pub relationship_status: RelationshipKnowledgeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedBusinessApplicationForServer {
    pub business_application: SnowRecord,
    pub provenance: String,
    pub min_depth: usize,
    #[serde(default)]
    pub paths: Vec<BusinessApplicationServerPath>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_health: Option<BusinessApplicationServerInventoryHealth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EndpointResolutionStatus {
    #[default]
    CacheHit,
    LiveConfirmed,
    NotFoundLiveConfirmed,
    LiveLookupFailed,
    LiveConfirmationNotAttempted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipKnowledgeStatus {
    #[default]
    KnownRelationships,
    NoCachedRelationships,
    UnknownNotSynced,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServerInventoryHealth {
    pub ba_sys_id: String,
    pub run_started_at: DateTime<Utc>,
    pub run_completed_at: DateTime<Utc>,
    pub service_membership_status: String,
    pub relationship_status: String,
    pub inventory_status: String,
    #[serde(default)]
    pub summary: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BusinessApplicationServerProvenance {
    Relationship,
    ServiceMembership,
    Both,
}

impl BusinessApplicationServerProvenance {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Relationship => "relationship",
            Self::ServiceMembership => "service_membership",
            Self::Both => "both",
        }
    }

    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Relationship, Self::ServiceMembership)
            | (Self::ServiceMembership, Self::Relationship)
            | (Self::Both, _)
            | (_, Self::Both) => Self::Both,
            (Self::Relationship, Self::Relationship) => Self::Relationship,
            (Self::ServiceMembership, Self::ServiceMembership) => Self::ServiceMembership,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServerApplication {
    pub sys_id: String,
    pub number: String,
    pub name: String,
    pub table: String,
}

impl From<&BusinessApplication> for BusinessApplicationServerApplication {
    fn from(application: &BusinessApplication) -> Self {
        Self {
            sys_id: application.record.sys_id.clone(),
            number: application.record.number.clone(),
            name: application.name.clone(),
            table: application.record.table.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BusinessApplicationServersSummary {
    pub max_depth: usize,
    pub max_cis: usize,
    pub max_edges: usize,
    pub max_service_membership_associations: usize,
    pub max_service_membership_pages: usize,
    #[serde(default)]
    pub servers_found: usize,
    #[serde(default)]
    pub relationships_examined: usize,
    #[serde(default)]
    pub service_membership_associations_examined: usize,
    #[serde(default)]
    pub service_membership_pages_examined: usize,
    #[serde(default)]
    pub cis_examined: usize,
    #[serde(default)]
    pub depth_limit_reached: bool,
    #[serde(default)]
    pub ci_limit_reached: bool,
    #[serde(default)]
    pub edge_limit_reached: bool,
    #[serde(default)]
    pub service_membership_association_limit_reached: bool,
    #[serde(default)]
    pub service_membership_page_limit_reached: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub truncated_count: usize,
    #[serde(default)]
    pub acl_restricted_count: usize,
    #[serde(default)]
    pub relationship_acl_restricted_count: usize,
    #[serde(default)]
    pub missing_ci_count: usize,
    #[serde(default)]
    pub cycle_count: usize,
    #[serde(default)]
    pub degraded_reasons: BTreeMap<String, usize>,
    #[serde(default)]
    pub persisted_servers: usize,
    #[serde(default)]
    pub membership_upserts: usize,
    #[serde(default)]
    pub membership_pruned: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_membership_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inventory_status: Option<String>,
    /// Pre-fallback `cmdb_rel_ci`/service-membership traversal server count.
    /// Present (and equal to `servers_found` minus any fallback servers) ONLY
    /// when a fallback strategy was requested; `None` (omitted) for the default
    /// `fallback_strategy = none` path so the default response shape is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmdb_servers_found: Option<usize>,
    /// Whether the `ci_owner_group` fallback actually fired. Only meaningful when
    /// a fallback strategy was requested. False/omitted on the default path.
    #[serde(default, skip_serializing_if = "is_false")]
    pub fallback_used: bool,
    /// Requested fallback strategy as a stable string. Present only when a
    /// fallback strategy other than `none` was requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_strategy: Option<String>,
    /// The BA's raw `u_ci_owner_group` sys_id used to drive the fallback query.
    /// Present only when the fallback fired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_group_sys_id: Option<String>,
    /// Display name of the fallback CI owner group, when known. Present only when
    /// the fallback fired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_group_display_name: Option<String>,
}

/// serde `skip_serializing_if` predicate for `bool` fields that should be omitted
/// when false, keeping the default (no-fallback) response shape unchanged.
fn is_false(value: &bool) -> bool {
    !*value
}

impl BusinessApplicationServersSummary {
    /// Build a fresh summary seeded with the traversal bounds. Every count/flag
    /// starts at its zero/false default; only the configured `max_*` limits carry
    /// non-default values, so the rest is filled from `Default` to avoid spelling
    /// out (and risking drift on) every counter by hand.
    pub fn new(options: &BusinessApplicationServersOptions) -> Self {
        Self {
            max_depth: options.max_depth,
            max_cis: options.max_cis,
            max_edges: options.max_edges,
            max_service_membership_associations: options.max_service_membership_associations,
            max_service_membership_pages: options.max_service_membership_pages,
            ..Default::default()
        }
    }

    pub fn record_degraded_reason(&mut self, reason: &ReferenceResolutionReason) {
        *self
            .degraded_reasons
            .entry(reason.as_key().to_string())
            .or_insert(0) += 1;
    }

    pub fn mark_truncated(&mut self, skipped: usize) {
        self.truncated = true;
        self.truncated_count += skipped;
    }

    /// Surface the CMDB data-quality gap raised by the `ci_owner_group` fallback.
    ///
    /// Sets the `cmdb_relationships_unmapped` entry in `degraded_reasons` to a
    /// truthy value. The map is typed `BTreeMap<String, usize>` (it counts
    /// reference-resolution reasons), so this records `1` rather than a JSON
    /// boolean; consumers treat a `> 0` value under this key as the flag being
    /// set. This is a live-summary-only signal and is never written into the
    /// durable inventory-health marker.
    pub fn record_cmdb_relationships_unmapped(&mut self) {
        self.degraded_reasons.insert(
            BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED.to_string(),
            1,
        );
    }
}

/// A single route from the root Business Application to a server, expressed as
/// an ordered list of traversed edges.
///
/// The path length (`depth` in CLI/MCP terms) is always exactly `edges.len()`,
/// so it is exposed as the [`Self::depth`] method rather than stored — a stored
/// copy could only ever drift out of sync with the edges it describes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServerPath {
    #[serde(default)]
    pub edges: Vec<BusinessApplicationServerPathEdge>,
}

impl BusinessApplicationServerPath {
    /// Number of edges in this route (equivalently, the BFS depth of the
    /// server relative to the root Business Application).
    pub fn depth(&self) -> usize {
        self.edges.len()
    }

    /// Alias for [`Self::depth`]; the route length is the edge count.
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Whether this route has no edges (only the root would have an empty route).
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

/// One traversed `cmdb_rel_ci` edge along a [`BusinessApplicationServerPath`].
///
/// The edge stores the relationship as its raw CMDB orientation
/// (`parent_sys_id`/`child_sys_id`) plus the `direction` in which the BFS
/// crossed it. The traversal endpoints (`from`/`to`) are fully derivable from
/// those three fields and are therefore exposed as the [`Self::from_sys_id`] /
/// [`Self::to_sys_id`] accessors instead of being stored redundantly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServerPathEdge {
    pub depth: usize,
    pub parent_sys_id: String,
    pub child_sys_id: String,
    pub direction: BusinessApplicationRelationshipDirection,
    pub relationship_type: BusinessApplicationRelationshipType,
    #[serde(
        default,
        skip_serializing_if = "BusinessApplicationServerPathEdgeSource::is_relationship"
    )]
    pub edge_source: BusinessApplicationServerPathEdgeSource,
}

impl BusinessApplicationServerPathEdge {
    /// The CI the BFS traversal entered this edge FROM.
    ///
    /// For a parent→child crossing this is the parent; for a child→parent
    /// crossing it is the child. Derived from `direction` so it can never
    /// disagree with the stored CMDB orientation.
    pub fn from_sys_id(&self) -> &str {
        match self.direction {
            BusinessApplicationRelationshipDirection::ParentToChild => &self.parent_sys_id,
            BusinessApplicationRelationshipDirection::ChildToParent => &self.child_sys_id,
        }
    }

    /// The CI the BFS traversal exited this edge TO (the newly reached CI).
    ///
    /// The mirror of [`Self::from_sys_id`].
    pub fn to_sys_id(&self) -> &str {
        match self.direction {
            BusinessApplicationRelationshipDirection::ParentToChild => &self.child_sys_id,
            BusinessApplicationRelationshipDirection::ChildToParent => &self.parent_sys_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BusinessApplicationServerPathEdgeSource {
    #[default]
    Relationship,
    ServiceMembership,
}

impl BusinessApplicationServerPathEdgeSource {
    pub fn is_relationship(&self) -> bool {
        matches!(self, Self::Relationship)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BusinessApplicationRelationshipDirection {
    ParentToChild,
    ChildToParent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationRelationshipType {
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_value: Option<String>,
}

impl BusinessApplicationRelationshipType {
    pub fn matches_any(&self, allowed: &[String]) -> bool {
        if allowed.is_empty() {
            return true;
        }
        let raw = normalize_relationship_type_match_value(&self.value);
        let display = self
            .display_value
            .as_deref()
            .map(normalize_relationship_type_match_value);
        allowed.iter().any(|allowed| {
            let allowed = normalize_relationship_type_match_value(allowed);
            raw == allowed || display.as_deref() == Some(allowed.as_str())
        })
    }
}

impl BusinessApplication {
    pub fn from_servicenow(
        record: &Record,
        aliases: &BusinessApplicationFieldAliases,
    ) -> Result<Self> {
        let mut snow_record = SnowRecord::from_servicenow(record);
        snow_record.references = collect_business_application_references(record, aliases);
        snow_record.source = CacheSource::Api;

        let name = business_application_display_name(record);
        let fields = business_application_fields(record, aliases);
        let references = business_application_reference_descriptors(record, aliases, &fields);
        let mut unresolved_references = aliases.diagnostics.clone();
        unresolved_references.extend(
            references
                .iter()
                .filter(|reference| {
                    reference.resolution_status != ReferenceResolutionStatus::Resolved
                })
                .map(|reference| ReferenceResolutionDiagnostic {
                    field: reference.field.clone(),
                    reference_table: reference.reference_table.clone(),
                    reference_sys_id: reference.reference_sys_id.clone(),
                    display_value: reference.display_value.clone(),
                    reason: match reference.resolution_status {
                        ReferenceResolutionStatus::UnknownTable => {
                            ReferenceResolutionReason::UnknownReferenceTable
                        }
                        ReferenceResolutionStatus::Unresolved => {
                            ReferenceResolutionReason::DictionaryUnavailable
                        }
                        ReferenceResolutionStatus::Resolved => {
                            ReferenceResolutionReason::ReferenceResolutionFailed
                        }
                        ReferenceResolutionStatus::NotFound => {
                            ReferenceResolutionReason::ReferenceNotFound
                        }
                        ReferenceResolutionStatus::AclRestricted => {
                            ReferenceResolutionReason::ReferenceAclRestricted
                        }
                        ReferenceResolutionStatus::Error => {
                            ReferenceResolutionReason::ReferenceResolutionFailed
                        }
                    },
                    message: reference.diagnostic.clone(),
                }),
        );

        Ok(Self {
            business_owner: snow_record.references.get(&aliases.business_owner).cloned(),
            is_owner: snow_record.references.get(&aliases.is_owner).cloned(),
            ci_owner_group: snow_record.references.get(&aliases.ci_owner_group).cloned(),
            primary_support_group: snow_record
                .references
                .get(&aliases.primary_support_group)
                .cloned(),
            operational_state: choice_value(record, &aliases.operational_state),
            primary_portfolio: snow_record
                .references
                .get(&aliases.primary_portfolio)
                .cloned(),
            attested_date: record
                .get_raw(&aliases.attested_date)
                .or_else(|| record.get_display(&aliases.attested_date))
                .or_else(|| record.get_str(&aliases.attested_date))
                .map(ToOwned::to_owned),
            field_dictionary_version: aliases.dictionary_version.clone(),
            fields,
            references,
            unresolved_references,
            name,
            record: snow_record,
        })
    }

    /// Resolve the raw `u_ci_owner_group` reference (sys_id + display name) that
    /// drives the `ci_owner_group` CMDB-gap fallback.
    ///
    /// This deliberately reads the raw [`BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD`]
    /// column captured in `self.fields`, NOT the [`Self::ci_owner_group`] typed
    /// alias. The typed alias maps to `managed_by_group`, which is
    /// present-but-empty on live BA records; sourcing the fallback from it would
    /// make the fallback silently never fire. Returns `None` when the BA has no
    /// owner-group value set (the field is absent or empty), which the caller
    /// surfaces as a clean diagnostic rather than an error.
    pub fn ci_owner_group_raw(&self) -> Option<CiOwnerGroupRef> {
        let field = self
            .fields
            .get(BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD)?;
        let sys_id = field
            .value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)?;
        let display_name = field
            .display_value
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        Some(CiOwnerGroupRef {
            sys_id,
            display_name,
        })
    }
}

/// Minimal owner-group reference extracted from the BA's raw `u_ci_owner_group`
/// column for the `ci_owner_group` fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiOwnerGroupRef {
    pub sys_id: String,
    pub display_name: Option<String>,
}

fn validate_bound(name: &str, value: Option<usize>, default: usize, max: usize) -> Result<usize> {
    let value = value.unwrap_or(default);
    if value == 0 {
        anyhow::bail!("`{name}` must be at least 1");
    }
    if value > max {
        anyhow::bail!("`{name}` must be at most {max}");
    }
    Ok(value)
}

fn normalize_business_application_number(number: &str) -> Result<String> {
    let number = number.trim();
    if number.to_ascii_uppercase().starts_with("BA:") {
        anyhow::bail!("`number` must be a real Business Application number, not BA:<sys_id>");
    }
    let normalized = number.to_ascii_uppercase();
    let suffix = normalized.strip_prefix("APM").unwrap_or_default();
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        anyhow::bail!("`number` must be a real Business Application number such as <APM_NUMBER>");
    }
    Ok(normalized)
}

fn normalized_relationship_types(values: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        let Some(value) = non_empty_owned(Some(value)) else {
            continue;
        };
        if !out.iter().any(|existing| existing == &value) {
            out.push(value);
        }
    }
    out
}

fn normalize_relationship_type_match_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BusinessApplicationLookup {
    SysId(String),
    ExactName(String),
}

impl BusinessApplicationLookup {
    pub fn sys_id(sys_id: impl AsRef<str>) -> Result<Self> {
        Ok(Self::SysId(normalize_record_lookup_sys_id(
            sys_id.as_ref(),
        )?))
    }

    pub fn exact_name(name: impl Into<String>) -> Self {
        Self::ExactName(name.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationHydrationOptions {
    pub persist: bool,
    pub resolve_references: bool,
    pub reference_depth: usize,
    pub refresh_dictionary: bool,
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

/// Aggregate result of a Business Application sync run.
///
/// A sync runs a live ServiceNow search (and persistence, when enabled) and then
/// rolls up how many applications were processed, how their reference fields
/// resolved, and whether dictionary metadata was available. The CLI/daemon expose
/// this serialized as the `summary` object of `business_application_sync`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BusinessApplicationSyncSummary {
    /// True when the sync used the explicit all-record live drain path.
    #[serde(default)]
    pub all: bool,
    /// ServiceNow table synced by this run.
    #[serde(default)]
    pub table: String,
    /// Effective live page size for the run.
    #[serde(default)]
    pub page_size: usize,
    /// Count of live pages that returned records.
    #[serde(default)]
    pub pages: usize,
    /// Total Business Applications returned by the live API.
    #[serde(default)]
    pub total_returned: usize,
    /// Total Business Applications returned by the live search.
    #[serde(default)]
    pub total_applications: usize,
    /// Number of applications persisted to vault/cache during the run.
    ///
    /// Equals `total_applications` when `persist` is enabled; `0` for a
    /// non-persistent preview run.
    #[serde(default)]
    pub persisted: usize,
    /// Total reference fields across all synced applications that resolved to a
    /// local primitive object.
    #[serde(default)]
    pub references_resolved: usize,
    /// Total reference fields that could not be resolved (unknown table, ACL
    /// restriction, not found, dictionary-degraded inference, etc.). These are
    /// degraded reads, not failures.
    #[serde(default)]
    pub references_unresolved: usize,
    /// Whether the sync ran in dictionary-degraded mode (baseline aliases used
    /// because verified `sys_dictionary` metadata was unavailable).
    #[serde(default)]
    pub dictionary_degraded: bool,
    /// Per-reason counts of unresolved references, keyed by the snake_case
    /// `ReferenceResolutionReason` (e.g. `dictionary_unavailable`,
    /// `unknown_reference_table`). Useful for surfacing why a sync degraded.
    #[serde(default)]
    pub degraded_reasons: BTreeMap<String, usize>,
    /// Whether the run requested (and attempted) a live dictionary refresh.
    #[serde(default)]
    pub dictionary_refreshed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChoiceValue {
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationFieldValue {
    pub field: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_table: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferencePrimitiveDescriptor {
    pub field: String,
    pub reference_table: String,
    pub reference_sys_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_value: Option<String>,
    pub primitive_type: ReferencePrimitiveType,
    pub resolution_status: ReferenceResolutionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReferencePrimitiveType {
    UserPrimitive,
    GroupPrimitive,
    PortfolioPrimitive,
    ConfigurationItemPrimitive,
    ReferencedRecordPrimitive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceResolutionStatus {
    Resolved,
    Unresolved,
    UnknownTable,
    NotFound,
    AclRestricted,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceResolutionDiagnostic {
    pub field: String,
    pub reference_table: String,
    pub reference_sys_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_value: Option<String>,
    pub reason: ReferenceResolutionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceResolutionReason {
    DictionaryUnavailable,
    UnknownReferenceTable,
    ReferenceNotFound,
    ReferenceAclRestricted,
    ReferenceResolutionFailed,
    CycleDetected,
    FanoutLimitExceeded,
}

impl ReferenceResolutionReason {
    /// Stable snake_case key matching the serde representation. Used to key the
    /// degraded-reason counts in [`BusinessApplicationSyncSummary`].
    pub fn as_key(&self) -> &'static str {
        match self {
            Self::DictionaryUnavailable => "dictionary_unavailable",
            Self::UnknownReferenceTable => "unknown_reference_table",
            Self::ReferenceNotFound => "reference_not_found",
            Self::ReferenceAclRestricted => "reference_acl_restricted",
            Self::ReferenceResolutionFailed => "reference_resolution_failed",
            Self::CycleDetected => "cycle_detected",
            Self::FanoutLimitExceeded => "fanout_limit_exceeded",
        }
    }

    /// True when this diagnostic signals the dictionary itself was unavailable,
    /// which puts the whole sync into dictionary-degraded mode.
    pub fn is_dictionary_unavailable(&self) -> bool {
        matches!(self, Self::DictionaryUnavailable)
    }
}

/// Typed field alias map for the Business Application primitive.
///
/// Field names are owned `String`s so that dictionary-verified instance field names
/// (which may be custom `u_*` fields) can override the hardcoded baseline at
/// runtime. `dictionary_version` is populated only when the aliases were
/// resolved from cached `sys_dictionary` metadata; on a dictionary cache miss
/// the baseline map is used and a `DictionaryUnavailable` diagnostic is attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessApplicationFieldAliases {
    pub business_owner: String,
    pub is_owner: String,
    pub ci_owner_group: String,
    pub primary_support_group: String,
    pub operational_state: String,
    pub primary_portfolio: String,
    /// Dictionary-discovered target table for the Primary Portfolio reference,
    /// when known. Falls back to `pm_portfolio` during table inference.
    pub primary_portfolio_table: Option<String>,
    pub attested_date: String,
    pub dictionary_version: Option<String>,
    pub diagnostics: Vec<ReferenceResolutionDiagnostic>,
}

impl BusinessApplicationFieldAliases {
    pub fn baseline_degraded() -> Self {
        let mut aliases = Self::baseline();
        aliases.diagnostics.push(ReferenceResolutionDiagnostic {
            field: "cmdb_ci_business_app".to_string(),
            reference_table: "sys_dictionary".to_string(),
            reference_sys_id: String::new(),
            display_value: None,
            reason: ReferenceResolutionReason::DictionaryUnavailable,
            message: Some(
                "using baseline Business Application alias map; dictionary verification unavailable"
                    .to_string(),
            ),
        });
        aliases
    }

    pub fn baseline() -> Self {
        Self {
            business_owner: "business_owner".to_string(),
            is_owner: "it_application_owner".to_string(),
            ci_owner_group: "managed_by_group".to_string(),
            primary_support_group: "support_group".to_string(),
            operational_state: "operational_status".to_string(),
            primary_portfolio: "portfolio".to_string(),
            primary_portfolio_table: None,
            attested_date: "attested_date".to_string(),
            dictionary_version: None,
            diagnostics: Vec::new(),
        }
    }
}

pub fn is_business_application_alias(value: &str) -> bool {
    matches!(
        value
            .trim()
            .to_ascii_lowercase()
            .replace([' ', '-'], "_")
            .as_str(),
        BUSINESS_APPLICATION_RESOURCE_TYPE | "business_app" | BUSINESS_APPLICATION_TABLE
    )
}

pub fn business_application_number(record: &Record) -> String {
    record
        .get_raw("number")
        .or_else(|| record.get_display("number"))
        .or_else(|| record.get_str("number"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("BA:{}", record.sys_id))
}

pub fn business_application_display_name(record: &Record) -> String {
    first_record_value(record, &["name", "short_description"])
        .unwrap_or_else(|| record.sys_id.clone())
}

pub fn business_application_state(
    record: &Record,
    aliases: &BusinessApplicationFieldAliases,
) -> String {
    record
        .get_display(&aliases.operational_state)
        .or_else(|| record.get_raw(&aliases.operational_state))
        .or_else(|| record.get_str(&aliases.operational_state))
        .unwrap_or_default()
        .to_string()
}

pub fn business_application_description(record: &Record) -> String {
    first_record_value(record, &["short_description", "description"]).unwrap_or_default()
}

pub fn collect_business_application_references(
    record: &Record,
    aliases: &BusinessApplicationFieldAliases,
) -> HashMap<String, Reference> {
    let mut references = HashMap::new();
    for field_name in known_reference_fields(aliases) {
        if let Some(reference) =
            reference_from_business_application_field(record, &field_name, aliases)
        {
            references.insert(field_name, reference);
        }
    }

    for (field_name, value) in record.fields() {
        if references.contains_key(field_name) {
            continue;
        }
        if value.display_value.is_some()
            && value
                .value
                .as_ref()
                .and_then(Value::as_str)
                .is_some_and(looks_like_sys_id)
            && let Some(reference) =
                reference_from_business_application_field(record, field_name, aliases)
        {
            references.insert(field_name.clone(), reference);
        }
    }

    references
}

pub fn business_application_reference_descriptors(
    record: &Record,
    aliases: &BusinessApplicationFieldAliases,
    fields: &BTreeMap<String, BusinessApplicationFieldValue>,
) -> Vec<ReferencePrimitiveDescriptor> {
    collect_business_application_references(record, aliases)
        .into_iter()
        .map(|(field, reference)| {
            let reference_table = reference.table.clone();
            let primitive_type = primitive_type_for_table(&reference_table);
            let resolution_status = if matches!(
                primitive_type,
                ReferencePrimitiveType::ReferencedRecordPrimitive
            ) && !is_known_reference_table(&reference_table)
            {
                ReferenceResolutionStatus::UnknownTable
            } else {
                ReferenceResolutionStatus::Resolved
            };
            let diagnostic = if resolution_status == ReferenceResolutionStatus::UnknownTable {
                Some(
                    "reference table is not in the Business Application primitive allowlist"
                        .to_string(),
                )
            } else if fields
                .get(&field)
                .and_then(|field| field.reference_table.as_deref())
                .is_none()
            {
                Some(
                    "reference table came from baseline/shape inference, not dictionary metadata"
                        .to_string(),
                )
            } else {
                None
            };
            ReferencePrimitiveDescriptor {
                field,
                reference_table,
                reference_sys_id: reference.sys_id,
                display_value: (!reference.display_name.is_empty())
                    .then_some(reference.display_name),
                primitive_type,
                resolution_status,
                diagnostic,
            }
        })
        .collect()
}

fn business_application_fields(
    record: &Record,
    aliases: &BusinessApplicationFieldAliases,
) -> BTreeMap<String, BusinessApplicationFieldValue> {
    let mut fields = BTreeMap::new();
    for (field_name, field_value) in record.fields() {
        let raw = field_value.value.clone().unwrap_or(Value::Null);
        let normalized_text = field_value
            .display_value
            .as_deref()
            .or_else(|| raw.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase());
        let reference_table = reference_table_for_field(field_name, field_value, aliases);
        let field_type = if reference_table.is_some() {
            Some("reference".to_string())
        } else if field_name == &aliases.operational_state {
            Some("choice".to_string())
        } else {
            None
        };
        fields.insert(
            field_name.clone(),
            BusinessApplicationFieldValue {
                field: field_name.clone(),
                value: raw,
                display_value: field_value.display_value.clone(),
                normalized_text,
                field_type,
                reference_table,
            },
        );
    }
    fields
}

fn reference_from_business_application_field(
    record: &Record,
    field_name: &str,
    aliases: &BusinessApplicationFieldAliases,
) -> Option<Reference> {
    let field_value = record.get(field_name)?;
    let sys_id = field_value
        .value
        .as_ref()
        .and_then(Value::as_str)
        .or_else(|| record.get_raw(field_name))
        .filter(|value| looks_like_sys_id(value))?
        .to_string();
    let table = reference_table_for_field(field_name, field_value, aliases)
        .unwrap_or_else(|| "unknown".to_string());
    let display_name = choose_reference_display_name([
        field_value.display_value.as_deref(),
        record.get_str(&format!("{field_name}.name")),
        record.get_str(&format!("{field_name}.number")),
        record.get_str(&format!("{field_name}.user_name")),
        Some(sys_id.as_str()),
    ]);
    Some(Reference {
        sys_id,
        table,
        display_name,
        extra: HashMap::new(),
    })
}

fn choice_value(record: &Record, field_name: &str) -> Option<ChoiceValue> {
    let field = record.get(field_name)?;
    Some(ChoiceValue {
        value: field
            .value
            .as_ref()
            .map(|value| match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default(),
        display_value: field.display_value.clone(),
    })
}

fn first_record_value(record: &Record, fields: &[&str]) -> Option<String> {
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

fn known_reference_fields(aliases: &BusinessApplicationFieldAliases) -> Vec<String> {
    let mut fields = vec![
        aliases.business_owner.clone(),
        aliases.is_owner.clone(),
        aliases.ci_owner_group.clone(),
        aliases.primary_support_group.clone(),
        aliases.primary_portfolio.clone(),
        "cmdb_ci".to_string(),
        "owned_by".to_string(),
        "managed_by".to_string(),
        "assignment_group".to_string(),
    ];
    fields.sort_unstable();
    fields.dedup();
    fields
}

fn reference_table_for_field(
    field_name: &str,
    field_value: &servicenow_rs::prelude::FieldValue,
    aliases: &BusinessApplicationFieldAliases,
) -> Option<String> {
    if let Some(table) = reference_table_from_link(field_value.link.as_deref()) {
        return Some(table);
    }
    // The Primary Portfolio target table is dictionary-discovered; fall back to
    // the baseline portfolio table only when the dictionary did not supply one.
    if field_name == aliases.primary_portfolio.as_str() {
        return Some(
            aliases
                .primary_portfolio_table
                .clone()
                .unwrap_or_else(|| "pm_portfolio".to_string()),
        );
    }
    Some(
        match field_name {
            field if field == aliases.business_owner.as_str() => "sys_user",
            field if field == aliases.is_owner.as_str() => "sys_user",
            field if field == aliases.ci_owner_group.as_str() => "sys_user_group",
            field if field == aliases.primary_support_group.as_str() => "sys_user_group",
            "cmdb_ci" => "cmdb_ci",
            "owned_by" => "sys_user",
            "managed_by" => "sys_user",
            "assignment_group" => "sys_user_group",
            _ => return None,
        }
        .to_string(),
    )
}

fn reference_table_from_link(link: Option<&str>) -> Option<String> {
    let link = link?;
    let marker = "/api/now/table/";
    let start = link.find(marker)? + marker.len();
    let rest = &link[start..];
    let table = rest.split('/').next()?.trim();
    (!table.is_empty()).then(|| table.to_string())
}

fn primitive_type_for_table(table: &str) -> ReferencePrimitiveType {
    match table {
        "sys_user" => ReferencePrimitiveType::UserPrimitive,
        "sys_user_group" => ReferencePrimitiveType::GroupPrimitive,
        "cmdb_ci" | "cmdb_ci_business_app" => ReferencePrimitiveType::ConfigurationItemPrimitive,
        "unknown" => ReferencePrimitiveType::ReferencedRecordPrimitive,
        table if table.contains("portfolio") => ReferencePrimitiveType::PortfolioPrimitive,
        _ => ReferencePrimitiveType::ReferencedRecordPrimitive,
    }
}

fn is_known_reference_table(table: &str) -> bool {
    matches!(
        table,
        "sys_user" | "sys_user_group" | "cmdb_ci" | "cmdb_ci_business_app"
    ) || table.contains("portfolio")
}

fn looks_like_sys_id(value: &str) -> bool {
    let value = value.trim();
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[allow(dead_code)]
fn _field_value_from_business_field(field: &BusinessApplicationFieldValue) -> FieldValue {
    FieldValue {
        value: match &field.value {
            Value::String(text) => text.clone(),
            Value::Null => String::new(),
            other => other.to_string(),
        },
        display_value: field.display_value.clone(),
    }
}
