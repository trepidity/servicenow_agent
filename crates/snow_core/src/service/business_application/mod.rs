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

mod dictionary;
mod fallback;
mod inventory;
mod model;
mod persistence;
mod references;
mod sync;
mod topology;

use dictionary::*;
pub(crate) use model::BusinessApplicationService;
use references::*;

pub use topology::BusinessApplicationSearchParams;
use topology::*;

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

    /// Fetch one Business Application without consulting local cache, vault,
    /// derived indexes, or cached dictionary metadata.
    ///
    /// Cache policy owns persistence for this entry point. The baseline typed
    /// aliases are used deliberately: resolving instance aliases would itself
    /// consult the local dictionary projection, violating `live` mode.
    pub async fn get_business_application_policy_live(
        &self,
        lookup: BusinessApplicationLookup,
        persist: bool,
    ) -> Result<Option<BusinessApplication>> {
        let aliases = BusinessApplicationFieldAliases::baseline_degraded();
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
        let business_application = BusinessApplication::from_servicenow(&record, &aliases)?;
        if persist {
            self.ctx.persist_record(&record)?;
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

        self.search_business_applications_with_aliases(params, options, &aliases)
            .await
    }

    /// Run a policy-owned live search without consulting local cache, vault,
    /// indexes, or cached dictionary metadata before the ServiceNow request.
    pub async fn search_business_applications_policy_live(
        &self,
        params: BusinessApplicationSearchParams,
        persist: bool,
    ) -> Result<Vec<BusinessApplication>> {
        params.validate()?;
        self.search_business_applications_with_aliases(
            params,
            BusinessApplicationHydrationOptions {
                persist,
                resolve_references: false,
                reference_depth: 0,
                refresh_dictionary: false,
            },
            &BusinessApplicationFieldAliases::baseline_degraded(),
        )
        .await
    }

    async fn search_business_applications_with_aliases(
        &self,
        params: BusinessApplicationSearchParams,
        options: BusinessApplicationHydrationOptions,
        aliases: &BusinessApplicationFieldAliases,
    ) -> Result<Vec<BusinessApplication>> {
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
        self.hydrate_business_application_page(records, aliases, &options)
            .await
    }

    /// Execute the typed Business Application query against ServiceNow rather
    /// than the narrowed local projection. Cache policy owns persistence.
    pub async fn query_business_applications_policy_live(
        &self,
        query: BusinessApplicationQuery,
        persist: bool,
    ) -> Result<Vec<BusinessApplication>> {
        let limit = query.limit.unwrap_or(BUSINESS_APPLICATION_DEFAULT_LIMIT);
        if limit == 0 || limit > 500 {
            anyhow::bail!("Business Application query limit must be between 1 and 500");
        }
        let offset = query.offset.unwrap_or(0);
        let mut live = self
            .ctx
            .client
            .table(BUSINESS_APPLICATION_TABLE)
            .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(u32::try_from(limit)?)
            .offset(u32::try_from(offset)?);

        if let Some(text) = non_empty_owned(query.text.as_deref()) {
            live = live.contains("name", &text);
        }
        for filter in query.filters {
            let operator = match filter.op {
                crate::query::filter::FieldOperator::Eq => Operator::Equals,
                crate::query::filter::FieldOperator::Ne => Operator::NotEquals,
                crate::query::filter::FieldOperator::Contains => Operator::Contains,
                crate::query::filter::FieldOperator::StartsWith => Operator::StartsWith,
                crate::query::filter::FieldOperator::In => Operator::In,
                crate::query::filter::FieldOperator::IsEmpty => Operator::IsEmpty,
                crate::query::filter::FieldOperator::IsNotEmpty => Operator::IsNotEmpty,
                crate::query::filter::FieldOperator::Gt => Operator::GreaterThan,
                crate::query::filter::FieldOperator::Gte => Operator::GreaterThanOrEqual,
                crate::query::filter::FieldOperator::Lt => Operator::LessThan,
                crate::query::filter::FieldOperator::Lte => Operator::LessThanOrEqual,
            };
            let value = business_application_filter_value(&filter.value);
            live = live.filter(&filter.field, operator, &value);
        }
        for sort in query.sort {
            let order = match sort.direction {
                crate::query::filter::SortDirection::Asc => Order::Asc,
                crate::query::filter::SortDirection::Desc => Order::Desc,
            };
            live = live.order_by(&sort.field, order);
        }

        let records = live.execute().await?.records;
        self.hydrate_business_application_page(
            records,
            &BusinessApplicationFieldAliases::baseline_degraded(),
            &BusinessApplicationHydrationOptions {
                persist,
                resolve_references: false,
                reference_depth: 0,
                refresh_dictionary: false,
            },
        )
        .await
    }

    pub async fn query_business_applications(
        &self,
        query: BusinessApplicationQuery,
    ) -> Result<Vec<SnowRecord>> {
        self.ctx.query.query_business_applications(query).await
    }
}

fn business_application_filter_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(business_application_filter_value)
            .collect::<Vec<_>>()
            .join(","),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
