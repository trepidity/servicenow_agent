//! `ApprovalService` — approval workflow queries and actions (fetch, list,
//! approve, reject), extracted from the `SnowCore` god-object.
//!
//! Second domain service extracted in the library boundary migration
//! (Task 9), following the pattern `UserService` (Task 8) established. Every
//! method/helper/type/const body below is moved verbatim from its former
//! `impl SnowCore` / free-fn / module-level location in `lib.rs`.
//!
//! Unlike `UserService`, most bodies here DO need a receiver rewrite: several
//! call the shared I/O primitives that live on `CoreContext` (`persist_record`,
//! `persist_records`, `get_record_fresh`, `get_record_by_table_sys_id_fresh`,
//! `current_user_sys_id`, `resolve_user_sys_id`, `lookup_table_and_sys_id`).
//! In `lib.rs` these were reached through `SnowCore`'s own private convenience
//! wrappers (e.g. `fn persist_record(&self, ..) { self.ctx.persist_record(..) }`).
//! `ApprovalService` has no such wrappers of its own, so every one of those
//! calls is rewritten here from `self.<helper>(..)` to `self.ctx.<helper>(..)`.
//! Calls between approval helpers (e.g. `self.pending_approval_row_by_sys_id(..)`)
//! are untouched intra-service calls — no rewrite needed.
//!
//! `servicenow_reference_sys_id`, `servicenow_record_text`, and
//! `servicenow_record_raw_text` are shared with non-approval code elsewhere in
//! `lib.rs` (business-application relationship parsing), so they were
//! relocated to `crate::helpers` rather than duplicated or privatized in here.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::{DisplayValue, Order, Record};

use crate::context::CoreContext;
use crate::helpers::{
    first_non_empty_str, servicenow_record_raw_text, servicenow_record_text,
    servicenow_reference_sys_id,
};
use crate::resource::approval::ApprovalResource;
use crate::{
    RecordRef, Reference, SnowRecord, normalize_record_lookup_sys_id, normalize_record_lookup_table,
};

pub const APPROVAL_GROUP_IN_BATCH_SIZE: usize = 20;
const APPROVAL_QUERY_PAGE_SIZE: usize = 200;
const APPROVAL_QUERY_MAX_PAGES: usize = 50;
const APPROVAL_GROUP_MEMBERSHIP_PAGE_SIZE: usize = 500;
const APPROVAL_GROUP_MEMBERSHIP_MAX_PAGES: usize = 50;
const APPROVAL_LIST_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "state",
    "approver",
    "source_table",
    "sysapproval",
    "document_id",
    "due_date",
    "sys_created_on",
];
const APPROVAL_LIST_DOT_WALK: &[&str] = &[
    "approver.name",
    "approver.user_name",
    "approver.sys_class_name",
    "sysapproval.number",
    "sysapproval.short_description",
    "sysapproval.state",
    "sysapproval.sys_class_name",
];
const APPROVAL_GROUP_MEMBERSHIP_FIELDS: &[&str] = &["sys_id", "user", "group"];
const APPROVAL_GROUP_MEMBERSHIP_DOT_WALK: &[&str] = &["group.name"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRoutedVia {
    #[default]
    Direct,
    Group,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ApprovalQuerySummary {
    pub direct_approvals_found: usize,
    pub group_approvals_found: usize,
    pub total_approvals: usize,
    pub caller_group_memberships_resolved: usize,
    pub group_query_batches: usize,
    pub deduplication_removed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListMyApprovalsResponse {
    pub records: Vec<ApprovalRecord>,
    pub query_summary: ApprovalQuerySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalRecord {
    pub record: SnowRecord,
    pub approver: Reference,
    pub target: RecordRef,
    pub requested_at: DateTime<Utc>,
    pub due_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub routed_via: ApprovalRoutedVia,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver_group: Option<Reference>,
}

fn approval_group_reference_from_membership(record: &Record) -> Result<Reference> {
    let sys_id = servicenow_reference_sys_id(record, "group").ok_or_else(|| {
        anyhow::anyhow!(
            "sys_user_grmember row {} is missing a readable group reference",
            record.sys_id
        )
    })?;
    let display_name = first_non_empty_str([
        record.get_display("group"),
        record.get_str("group.name"),
        record.get_raw("group"),
        record.get_str("group"),
    ])
    .map(ToOwned::to_owned)
    .unwrap_or_else(|| sys_id.clone());
    Ok(Reference {
        sys_id,
        table: "sys_user_group".to_string(),
        display_name,
        extra: HashMap::new(),
    })
}

fn approval_group_reference_from_approval(
    record: &Record,
    groups_by_sys_id: &HashMap<String, Reference>,
) -> Result<Reference> {
    let sys_id = servicenow_reference_sys_id(record, "approver").ok_or_else(|| {
        anyhow::anyhow!(
            "group approval row {} is missing a readable approver reference",
            record.sys_id
        )
    })?;
    let approval_display = servicenow_record_text(record, "approver");
    let mut group = groups_by_sys_id.get(&sys_id).cloned().unwrap_or_else(|| {
        ApprovalResource::group_approver_reference(record).unwrap_or_else(|| Reference {
            sys_id: sys_id.clone(),
            table: "sys_user_group".to_string(),
            display_name: approval_display.clone().unwrap_or_else(|| sys_id.clone()),
            extra: HashMap::new(),
        })
    });
    group.table = "sys_user_group".to_string();
    if (group.display_name.trim().is_empty() || group.display_name == group.sys_id)
        && let Some(display) = approval_display
    {
        group.display_name = display;
    }
    Ok(group)
}

#[derive(Clone)]
pub(crate) struct ApprovalService {
    ctx: CoreContext,
}

impl ApprovalService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn get_approval(&self, number: &str) -> Result<Option<ApprovalRecord>> {
        self.ctx.query.get_approval(number).await
    }

    pub async fn my_approvals_fresh(&self) -> Result<Vec<ApprovalRecord>> {
        Ok(self.my_approvals_with_routing_fresh().await?.records)
    }

    pub async fn my_approvals_with_routing_fresh(&self) -> Result<ListMyApprovalsResponse> {
        let user_sys_id = self.ctx.current_user_sys_id().await?;
        let direct_rows = self
            .pending_approval_rows_for_approver(&user_sys_id)
            .await
            .map_err(|err| anyhow::anyhow!("direct approval query failure: {err}"))?;
        let group_memberships = self
            .approval_group_memberships_for_user(&user_sys_id)
            .await
            .map_err(|err| anyhow::anyhow!("group membership lookup failure: {err}"))?;

        let mut group_refs = group_memberships
            .iter()
            .map(|group| group.sys_id.as_str())
            .collect::<Vec<_>>();
        group_refs.sort_unstable();

        let mut group_rows = Vec::new();
        let mut group_query_batches = 0usize;
        for batch in group_refs.chunks(APPROVAL_GROUP_IN_BATCH_SIZE) {
            if batch.is_empty() {
                continue;
            }
            group_query_batches += 1;
            let mut rows = self
                .pending_approval_rows_for_groups(batch)
                .await
                .map_err(|err| anyhow::anyhow!("group approval query failure: {err}"))?;
            group_rows.append(&mut rows);
        }

        let summary = ApprovalQuerySummary {
            direct_approvals_found: direct_rows.len(),
            group_approvals_found: group_rows.len(),
            caller_group_memberships_resolved: group_memberships.len(),
            group_query_batches,
            ..ApprovalQuerySummary::default()
        };

        self.ctx.persist_records(&direct_rows)?;

        self.materialize_my_approvals_response(direct_rows, group_rows, group_memberships, summary)
    }

    pub async fn my_approvals(&self) -> Result<Vec<ApprovalRecord>> {
        self.ctx.query.my_approvals().await
    }

    pub async fn approve(&self, number: &str, comment: Option<&str>) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        let approver_sys_id = self
            .ctx
            .resolve_user_sys_id(&self.ctx.config.instance.user)
            .await?;
        let mut builder = self.ctx.client.approve(&table, &sys_id, &approver_sys_id);
        if let Some(comment) = comment {
            builder = builder.comment(comment);
        }
        builder.execute().await?;
        self.ctx.get_record_fresh(number).await
    }

    pub async fn approve_approval(
        &self,
        approval_sys_id: &str,
        comment: Option<&str>,
    ) -> Result<Option<SnowRecord>> {
        self.update_approval_row_state(approval_sys_id, "approved", comment)
            .await
    }

    pub async fn reject(&self, number: &str, reason: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        let approver_sys_id = self
            .ctx
            .resolve_user_sys_id(&self.ctx.config.instance.user)
            .await?;
        self.ctx
            .client
            .reject(&table, &sys_id, &approver_sys_id)
            .comment(reason)
            .execute()
            .await?;
        self.ctx.get_record_fresh(number).await
    }

    pub async fn reject_approval(
        &self,
        approval_sys_id: &str,
        reason: &str,
    ) -> Result<Option<SnowRecord>> {
        self.update_approval_row_state(approval_sys_id, "rejected", Some(reason))
            .await
    }

    async fn pending_approval_rows_for_approver(
        &self,
        approver_sys_id: &str,
    ) -> Result<Vec<Record>> {
        let mut paginator = self
            .ctx
            .client
            .table("sysapproval_approver")
            .equals("approver", approver_sys_id)
            .equals("state", "requested")
            .fields(APPROVAL_LIST_FIELDS)
            .dot_walk(APPROVAL_LIST_DOT_WALK)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .order_by("sys_created_on", Order::Desc)
            .limit(APPROVAL_QUERY_PAGE_SIZE as u32)
            .paginate()?;
        let mut records = Vec::new();
        let mut pages = 0usize;
        loop {
            if pages >= APPROVAL_QUERY_MAX_PAGES {
                if !paginator.is_done() {
                    anyhow::bail!(
                        "direct approval query truncated after {APPROVAL_QUERY_MAX_PAGES} pages"
                    );
                }
                break;
            }
            let Some(page) = paginator.next_page().await? else {
                break;
            };
            pages += 1;
            records.extend(page.records);
        }
        Ok(records)
    }

    async fn pending_approval_rows_for_groups(
        &self,
        group_sys_ids: &[&str],
    ) -> Result<Vec<Record>> {
        let mut paginator = self
            .ctx
            .client
            .table("sysapproval_approver")
            .in_list("approver", group_sys_ids)
            .equals("state", "requested")
            .fields(APPROVAL_LIST_FIELDS)
            .dot_walk(APPROVAL_LIST_DOT_WALK)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .order_by("sys_created_on", Order::Desc)
            .limit(APPROVAL_QUERY_PAGE_SIZE as u32)
            .paginate()?;
        let mut records = Vec::new();
        let mut pages = 0usize;
        loop {
            if pages >= APPROVAL_QUERY_MAX_PAGES {
                if !paginator.is_done() {
                    anyhow::bail!(
                        "group approval query truncated after {APPROVAL_QUERY_MAX_PAGES} pages"
                    );
                }
                break;
            }
            let Some(page) = paginator.next_page().await? else {
                break;
            };
            pages += 1;
            records.extend(page.records);
        }
        Ok(records)
    }

    async fn approval_group_memberships_for_user(
        &self,
        user_sys_id: &str,
    ) -> Result<Vec<Reference>> {
        let mut paginator = self
            .ctx
            .client
            .table("sys_user_grmember")
            .equals("user", user_sys_id)
            .fields(APPROVAL_GROUP_MEMBERSHIP_FIELDS)
            .dot_walk(APPROVAL_GROUP_MEMBERSHIP_DOT_WALK)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(APPROVAL_GROUP_MEMBERSHIP_PAGE_SIZE as u32)
            .paginate()?;
        let mut groups_by_sys_id = BTreeMap::new();
        let mut pages = 0usize;
        loop {
            if pages >= APPROVAL_GROUP_MEMBERSHIP_MAX_PAGES {
                if !paginator.is_done() {
                    anyhow::bail!(
                        "group membership lookup truncated after {APPROVAL_GROUP_MEMBERSHIP_MAX_PAGES} pages"
                    );
                }
                break;
            }
            let Some(page) = paginator.next_page().await? else {
                break;
            };
            pages += 1;
            for record in page.records {
                let group = approval_group_reference_from_membership(&record)?;
                groups_by_sys_id
                    .entry(group.sys_id.clone())
                    .or_insert(group);
            }
        }
        Ok(groups_by_sys_id.into_values().collect())
    }

    async fn pending_approval_row_by_sys_id(
        &self,
        approval_sys_id: &str,
    ) -> Result<Option<Record>> {
        let approval_sys_id = normalize_record_lookup_sys_id(approval_sys_id)?;
        self.ctx
            .client
            .table("sysapproval_approver")
            .equals("sys_id", &approval_sys_id)
            .fields(APPROVAL_LIST_FIELDS)
            .dot_walk(APPROVAL_LIST_DOT_WALK)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(1)
            .first()
            .await
            .map_err(anyhow::Error::from)
    }

    async fn update_approval_row_state(
        &self,
        approval_sys_id: &str,
        state: &str,
        comment: Option<&str>,
    ) -> Result<Option<SnowRecord>> {
        let approval_sys_id = normalize_record_lookup_sys_id(approval_sys_id)?;
        let Some(approval_row) = self
            .pending_approval_row_by_sys_id(&approval_sys_id)
            .await?
        else {
            return Ok(None);
        };
        let target = self
            .authorize_approval_row_for_current_user(&approval_row)
            .await?;

        let mut body = serde_json::json!({ "state": state });
        if let Some(comment) = comment {
            body["comments"] = serde_json::json!(comment);
        }
        let updated = self
            .ctx
            .client
            .table("sysapproval_approver")
            .display_value(DisplayValue::Both)
            .fields(APPROVAL_LIST_FIELDS)
            .update(&approval_sys_id, body)
            .await?;
        self.ctx.persist_record(&updated)?;

        if !target.sys_id.trim().is_empty()
            && let Ok(table) = normalize_record_lookup_table(&target.table)
            && let Ok(record) = self
                .ctx
                .get_record_by_table_sys_id_fresh(&table, &target.sys_id)
                .await
        {
            return Ok(record);
        }
        if !target.number.trim().is_empty() {
            return self.ctx.get_record_fresh(&target.number).await;
        }
        Ok(None)
    }

    async fn authorize_approval_row_for_current_user(&self, record: &Record) -> Result<RecordRef> {
        let state = servicenow_record_raw_text(record, "state")
            .unwrap_or_default()
            .to_ascii_lowercase();
        if state != "requested" {
            anyhow::bail!(
                "approval row {} is not pending; current state is {state}",
                record.sys_id
            );
        }

        let approver_sys_id = servicenow_reference_sys_id(record, "approver").ok_or_else(|| {
            anyhow::anyhow!(
                "approval row {} is missing a readable approver reference",
                record.sys_id
            )
        })?;
        let user_sys_id = self.ctx.current_user_sys_id().await?;
        if approver_sys_id == user_sys_id {
            let approver =
                ApprovalResource::approver_reference(record).unwrap_or_else(|| Reference {
                    sys_id: user_sys_id,
                    table: "sys_user".to_string(),
                    display_name: servicenow_record_text(record, "approver")
                        .unwrap_or_else(|| approver_sys_id.clone()),
                    extra: HashMap::new(),
                });
            return Ok(ApprovalResource::from_servicenow_with_routing(
                record,
                approver,
                ApprovalRoutedVia::Direct,
                None,
            )
            .target);
        }

        let group_memberships = self
            .approval_group_memberships_for_user(&user_sys_id)
            .await?;
        let groups_by_sys_id = group_memberships
            .into_iter()
            .map(|group| (group.sys_id.clone(), group))
            .collect::<HashMap<_, _>>();
        if groups_by_sys_id.contains_key(&approver_sys_id) {
            let group = approval_group_reference_from_approval(record, &groups_by_sys_id)?;
            return Ok(ApprovalResource::from_servicenow_with_routing(
                record,
                group.clone(),
                ApprovalRoutedVia::Group,
                Some(group),
            )
            .target);
        }

        anyhow::bail!(
            "approval row {} is not assigned to the current user or one of their groups",
            record.sys_id
        );
    }

    fn materialize_my_approvals_response(
        &self,
        direct_rows: Vec<Record>,
        group_rows: Vec<Record>,
        group_memberships: Vec<Reference>,
        mut query_summary: ApprovalQuerySummary,
    ) -> Result<ListMyApprovalsResponse> {
        let groups_by_sys_id = group_memberships
            .into_iter()
            .map(|group| (group.sys_id.clone(), group))
            .collect::<HashMap<_, _>>();
        let mut seen = HashSet::new();
        let mut records = Vec::new();

        for row in direct_rows {
            if !seen.insert(row.sys_id.clone()) {
                query_summary.deduplication_removed += 1;
                continue;
            }
            let approver = ApprovalResource::approver_reference(&row).ok_or_else(|| {
                anyhow::anyhow!(
                    "direct approval row {} is missing a readable approver reference",
                    row.sys_id
                )
            })?;
            records.push(ApprovalResource::from_servicenow_with_routing(
                &row,
                approver,
                ApprovalRoutedVia::Direct,
                None,
            ));
        }

        for row in group_rows {
            if !seen.insert(row.sys_id.clone()) {
                query_summary.deduplication_removed += 1;
                continue;
            }
            let group = approval_group_reference_from_approval(&row, &groups_by_sys_id)?;
            records.push(ApprovalResource::from_servicenow_with_routing(
                &row,
                group.clone(),
                ApprovalRoutedVia::Group,
                Some(group),
            ));
        }

        records.sort_by(|left, right| {
            left.target
                .number
                .cmp(&right.target.number)
                .then_with(|| left.record.number.cmp(&right.record.number))
        });
        query_summary.total_approvals = records.len();
        Ok(ListMyApprovalsResponse {
            records,
            query_summary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        core_for_mock_server_with_user, mock_server_test_lock, mount_empty_journal_fetch,
    };
    use wiremock::matchers::{body_partial_json, method, path, query_param, query_param_contains};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn my_approvals_with_routing_fresh_unions_direct_and_group_rows() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let user_sys_id = "11111111111111111111111111111111";
        let group_sys_id = "22222222222222222222222222222222";
        let direct_approval_sys_id = "33333333333333333333333333333333";
        let group_approval_sys_id = "44444444444444444444444444444444";
        let direct_target_sys_id = "55555555555555555555555555555555";
        let group_target_sys_id = "66666666666666666666666666666666";

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .and(query_param("sysparm_query", "user_name=test_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": user_sys_id,
                    "user_name": "test_user",
                    "name": "Example User"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sysapproval_approver"))
            .and(query_param_contains("sysparm_query", format!("approver={user_sys_id}")))
            .and(query_param_contains("sysparm_query", "state=requested"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": direct_approval_sys_id,
                    "number": "APPROVAL_DIRECT",
                    "state": "requested",
                    "approver": { "value": user_sys_id, "display_value": "Example User" },
                    "source_table": "change_request",
                    "sysapproval": { "value": direct_target_sys_id, "display_value": "CHANGE_DIRECT" },
                    "sysapproval.number": "CHANGE_DIRECT",
                    "sysapproval.short_description": "Direct approval target",
                    "sysapproval.state": "scheduled",
                    "sysapproval.sys_class_name": "change_request",
                    "sys_created_on": "2026-06-10 10:00:00"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user_grmember"))
            .and(query_param_contains(
                "sysparm_query",
                format!("user={user_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "77777777777777777777777777777777",
                    "user": { "value": user_sys_id, "display_value": "Example User" },
                    "group": { "value": group_sys_id, "display_value": "Example Approval Group" },
                    "group.name": "Example Approval Group"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sysapproval_approver"))
            .and(query_param_contains("sysparm_query", format!("approverIN{group_sys_id}")))
            .and(query_param_contains("sysparm_query", "state=requested"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": direct_approval_sys_id,
                        "number": "APPROVAL_DIRECT",
                        "state": "requested",
                        "approver": { "value": group_sys_id, "display_value": "Example Approval Group" },
                        "source_table": "change_request",
                        "sysapproval": { "value": direct_target_sys_id, "display_value": "CHANGE_DIRECT" },
                        "sysapproval.number": "CHANGE_DIRECT",
                        "sysapproval.short_description": "Duplicate direct approval target",
                        "sysapproval.state": "scheduled",
                        "sysapproval.sys_class_name": "change_request",
                        "sys_created_on": "2026-06-10 10:00:00"
                    },
                    {
                        "sys_id": group_approval_sys_id,
                        "number": "APPROVAL_GROUP",
                        "state": "requested",
                        "approver": { "value": group_sys_id, "display_value": "Example Approval Group" },
                        "source_table": "change_request",
                        "sysapproval": { "value": group_target_sys_id, "display_value": "CHANGE_GROUP" },
                        "sysapproval.number": "CHANGE_GROUP",
                        "sysapproval.short_description": "Group approval target",
                        "sysapproval.state": "scheduled",
                        "sysapproval.sys_class_name": "change_request",
                        "sys_created_on": "2026-06-10 10:01:00"
                    }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let response = core
            .my_approvals_with_routing_fresh()
            .await
            .expect("approvals");

        assert_eq!(response.records.len(), 2);
        assert_eq!(response.query_summary.direct_approvals_found, 1);
        assert_eq!(response.query_summary.group_approvals_found, 2);
        assert_eq!(response.query_summary.total_approvals, 2);
        assert_eq!(response.query_summary.caller_group_memberships_resolved, 1);
        assert_eq!(response.query_summary.group_query_batches, 1);
        assert_eq!(response.query_summary.deduplication_removed, 1);

        let direct = response
            .records
            .iter()
            .find(|approval| approval.record.sys_id == direct_approval_sys_id)
            .expect("direct approval");
        assert_eq!(direct.routed_via, ApprovalRoutedVia::Direct);
        assert!(direct.approver_group.is_none());
        assert_eq!(direct.approver.table, "sys_user");

        let group = response
            .records
            .iter()
            .find(|approval| approval.record.sys_id == group_approval_sys_id)
            .expect("group approval");
        assert_eq!(group.routed_via, ApprovalRoutedVia::Group);
        assert_eq!(group.approver.table, "sys_user_group");
        assert_eq!(group.approver.sys_id, group_sys_id);
        assert_eq!(
            group.approver_group.as_ref().map(|approver_group| (
                approver_group.table.as_str(),
                approver_group.sys_id.as_str(),
                approver_group.display_name.as_str()
            )),
            Some(("sys_user_group", group_sys_id, "Example Approval Group"))
        );
    }

    #[tokio::test]
    async fn approve_approval_updates_pending_direct_row_by_sys_id() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let user_sys_id = "11111111111111111111111111111111";
        let approval_sys_id = "22222222222222222222222222222222";
        let target_sys_id = "33333333333333333333333333333333";

        Mock::given(method("GET"))
            .and(path("/api/now/table/sysapproval_approver"))
            .and(query_param_contains(
                "sysparm_query",
                format!("sys_id={approval_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": approval_sys_id,
                    "number": "APPROVAL_DIRECT",
                    "state": "requested",
                    "approver": { "value": user_sys_id, "display_value": "Example User" },
                    "source_table": "change_request",
                    "sysapproval": { "value": target_sys_id, "display_value": "CHANGE0010001" },
                    "sysapproval.number": "CHANGE0010001",
                    "sysapproval.short_description": "Direct approval target",
                    "sysapproval.state": "scheduled",
                    "sysapproval.sys_class_name": "change_request",
                    "sys_created_on": "2026-06-10 10:00:00"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .and(query_param("sysparm_query", "user_name=test_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": user_sys_id,
                    "user_name": "test_user",
                    "name": "Example User"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path(format!(
                "/api/now/table/sysapproval_approver/{approval_sys_id}"
            )))
            .and(body_partial_json(
                serde_json::json!({ "state": "approved" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": approval_sys_id,
                    "number": "APPROVAL_DIRECT",
                    "state": "approved",
                    "approver": { "value": user_sys_id, "display_value": "Example User" },
                    "source_table": "change_request",
                    "sysapproval": { "value": target_sys_id, "display_value": "CHANGE0010001" },
                    "sys_created_on": "2026-06-10 10:00:00"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!(
                "/api/now/table/change_request/{target_sys_id}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": target_sys_id,
                    "number": "CHANGE0010001",
                    "short_description": "Direct approval target",
                    "state": "scheduled"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        mount_empty_journal_fetch(&server, "change_request", target_sys_id).await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let record = core
            .approve_approval(approval_sys_id, None)
            .await
            .expect("approval action")
            .expect("target record");

        assert_eq!(record.number, "CHANGE0010001");
        let requests = server.received_requests().await.expect("requests");
        assert!(
            requests.iter().all(|request| {
                !request
                    .url
                    .query()
                    .unwrap_or_default()
                    .contains("(document_id=")
            }),
            "approval_sys_id path must not use target/approver reverse lookup"
        );
    }

    #[tokio::test]
    async fn reject_approval_allows_current_user_group_row_by_sys_id() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let user_sys_id = "11111111111111111111111111111111";
        let group_sys_id = "22222222222222222222222222222222";
        let membership_sys_id = "33333333333333333333333333333333";
        let approval_sys_id = "44444444444444444444444444444444";
        let target_sys_id = "55555555555555555555555555555555";

        Mock::given(method("GET"))
            .and(path("/api/now/table/sysapproval_approver"))
            .and(query_param_contains(
                "sysparm_query",
                format!("sys_id={approval_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": approval_sys_id,
                    "number": "APPROVAL_GROUP",
                    "state": "requested",
                    "approver": { "value": group_sys_id, "display_value": "Example Approval Group" },
                    "source_table": "change_request",
                    "sysapproval": { "value": target_sys_id, "display_value": "CHANGE0010002" },
                    "sysapproval.number": "CHANGE0010002",
                    "sysapproval.short_description": "Group approval target",
                    "sysapproval.state": "scheduled",
                    "sysapproval.sys_class_name": "change_request",
                    "sys_created_on": "2026-06-10 10:01:00"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .and(query_param("sysparm_query", "user_name=test_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": user_sys_id,
                    "user_name": "test_user",
                    "name": "Example User"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user_grmember"))
            .and(query_param_contains(
                "sysparm_query",
                format!("user={user_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": membership_sys_id,
                    "user": { "value": user_sys_id, "display_value": "Example User" },
                    "group": { "value": group_sys_id, "display_value": "Example Approval Group" },
                    "group.name": "Example Approval Group"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path(format!(
                "/api/now/table/sysapproval_approver/{approval_sys_id}"
            )))
            .and(body_partial_json(serde_json::json!({
                "state": "rejected",
                "comments": "Insufficient detail."
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": approval_sys_id,
                    "number": "APPROVAL_GROUP",
                    "state": "rejected",
                    "approver": { "value": group_sys_id, "display_value": "Example Approval Group" },
                    "source_table": "change_request",
                    "sysapproval": { "value": target_sys_id, "display_value": "CHANGE0010002" },
                    "sys_created_on": "2026-06-10 10:01:00"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!(
                "/api/now/table/change_request/{target_sys_id}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": target_sys_id,
                    "number": "CHANGE0010002",
                    "short_description": "Group approval target",
                    "state": "scheduled"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;
        mount_empty_journal_fetch(&server, "change_request", target_sys_id).await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let record = core
            .reject_approval(approval_sys_id, "Insufficient detail.")
            .await
            .expect("approval action")
            .expect("target record");

        assert_eq!(record.number, "CHANGE0010002");
    }

    #[tokio::test]
    async fn my_approvals_with_routing_fresh_fails_closed_when_group_lookup_fails() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let user_sys_id = "11111111111111111111111111111111";

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .and(query_param("sysparm_query", "user_name=test_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": user_sys_id,
                    "user_name": "test_user",
                    "name": "Example User"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sysapproval_approver"))
            .and(query_param_contains(
                "sysparm_query",
                format!("approver={user_sys_id}"),
            ))
            .and(query_param_contains("sysparm_query", "state=requested"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "33333333333333333333333333333333",
                    "number": "APPROVAL_DIRECT",
                    "state": "requested",
                    "approver": { "value": user_sys_id, "display_value": "Example User" },
                    "source_table": "change_request",
                    "sysapproval": { "value": "55555555555555555555555555555555", "display_value": "CHANGE_DIRECT" },
                    "sysapproval.number": "CHANGE_DIRECT",
                    "sysapproval.short_description": "Direct approval target",
                    "sysapproval.state": "scheduled",
                    "sysapproval.sys_class_name": "change_request",
                    "sys_created_on": "2026-06-10 10:00:00"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user_grmember"))
            .and(query_param_contains(
                "sysparm_query",
                format!("user={user_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {
                    "message": "denied",
                    "detail": "sys_user_grmember read denied"
                },
                "status": "failure"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let err = core
            .my_approvals_with_routing_fresh()
            .await
            .expect_err("group lookup failure must fail closed");
        assert!(
            err.to_string().contains("group membership lookup failure"),
            "{err}"
        );
    }
}
