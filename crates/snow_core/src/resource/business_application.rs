use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use servicenow_rs::prelude::Record;

use super::server::Server;
use crate::{
    CacheSource, FieldValue, Reference, SnowRecord, normalize_record_lookup_sys_id,
    reference::choose_reference_display_name,
};

pub const BUSINESS_APPLICATION_TABLE: &str = "cmdb_ci_business_app";
pub const BUSINESS_APPLICATION_RESOURCE_TYPE: &str = "business_application";
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH: usize = 2;
pub const BUSINESS_APPLICATION_SERVERS_MAX_DEPTH: usize = 4;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS: usize = 500;
pub const BUSINESS_APPLICATION_SERVERS_MAX_CIS: usize = 5000;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES: usize = 2000;
pub const BUSINESS_APPLICATION_SERVERS_MAX_EDGES: usize = 20000;
pub const BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES: &[&str] = &[
    "Depends on::Used by",
    "Runs on::Runs",
    "Contains::Contained by",
    "Hosted on::Hosts",
];

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relationship_type: Vec<String>,
    #[serde(default)]
    pub include_paths: bool,
}

impl BusinessApplicationServersParams {
    pub fn validate(&self) -> Result<BusinessApplicationServersOptions> {
        let number = non_empty_string(self.number.as_deref());
        let sys_id = non_empty_string(self.sys_id.as_deref());
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
                    relationship_type: normalized_relationship_types(&self.relationship_type),
                    include_paths: self.include_paths,
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
                relationship_type: normalized_relationship_types(&self.relationship_type),
                include_paths: self.include_paths,
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
    #[serde(default)]
    pub relationship_type: Vec<String>,
    #[serde(default)]
    pub include_paths: bool,
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
    pub relationship_summary: BusinessApplicationServersSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ReferenceResolutionDiagnostic>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub server_paths: BTreeMap<String, Vec<BusinessApplicationServerPath>>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServersSummary {
    pub max_depth: usize,
    pub max_cis: usize,
    pub max_edges: usize,
    #[serde(default)]
    pub servers_found: usize,
    #[serde(default)]
    pub relationships_examined: usize,
    #[serde(default)]
    pub cis_examined: usize,
    #[serde(default)]
    pub depth_limit_reached: bool,
    #[serde(default)]
    pub ci_limit_reached: bool,
    #[serde(default)]
    pub edge_limit_reached: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub truncated_count: usize,
    #[serde(default)]
    pub acl_restricted_count: usize,
    #[serde(default)]
    pub missing_ci_count: usize,
    #[serde(default)]
    pub cycle_count: usize,
    #[serde(default)]
    pub degraded_reasons: BTreeMap<String, usize>,
}

impl BusinessApplicationServersSummary {
    pub fn new(options: &BusinessApplicationServersOptions) -> Self {
        Self {
            max_depth: options.max_depth,
            max_cis: options.max_cis,
            max_edges: options.max_edges,
            servers_found: 0,
            relationships_examined: 0,
            cis_examined: 0,
            depth_limit_reached: false,
            ci_limit_reached: false,
            edge_limit_reached: false,
            truncated: false,
            truncated_count: 0,
            acl_restricted_count: 0,
            missing_ci_count: 0,
            cycle_count: 0,
            degraded_reasons: BTreeMap::new(),
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServerPath {
    pub depth: usize,
    #[serde(default)]
    pub edges: Vec<BusinessApplicationServerPathEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BusinessApplicationServerPathEdge {
    pub depth: usize,
    pub from_sys_id: String,
    pub to_sys_id: String,
    pub parent_sys_id: String,
    pub child_sys_id: String,
    pub direction: BusinessApplicationRelationshipDirection,
    pub relationship_type: BusinessApplicationRelationshipType,
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
        let Some(value) = non_empty_string(Some(value)) else {
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

fn non_empty_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
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
