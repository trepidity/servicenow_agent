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

mod topology;

pub use topology::BusinessApplicationSearchParams;
use topology::*;

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
mod tests;
