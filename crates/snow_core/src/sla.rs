use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::{Record, TaskSla, TaskSlaStage, TaskSlaSummary};

use crate::SnowCore;

const TASK_SLA_APPLICABLE_TABLES: &[&str] = &[
    "task",
    "incident",
    "change_request",
    "change_task",
    "problem",
    "sc_request",
    "sc_req_item",
    "sc_task",
    "rm_story",
    "rm_scrum_task",
];

/// Product-layer Task SLA status for a single ServiceNow parent record.
///
/// This type is the stable `snow_core` contract for CLI, TUI, and daemon
/// consumers. It intentionally does not expose raw `servicenow_rs::TaskSla`
/// values outside `snow_core`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSlaStatus {
    /// ServiceNow record number requested or resolved for the parent record.
    pub record_number: String,
    /// ServiceNow table name for the parent record.
    pub record_table: String,
    /// Raw ServiceNow sys_id for the parent record.
    pub record_sys_id: String,
    /// Readable Task SLA rows mapped to product-layer fields.
    pub rows: Vec<TaskSlaView>,
    /// Summary derived only from readable rows.
    pub summary: TaskSlaSummaryView,
    /// Whether Task SLA rows were readable, ambiguous, unavailable, or not applicable.
    pub readable: TaskSlaReadability,
}

/// Known parent identity for a bulk Task SLA lookup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSlaParentRef {
    /// ServiceNow record number for the parent record.
    pub record_number: String,
    /// ServiceNow table name for the parent record.
    pub record_table: String,
    /// Raw ServiceNow sys_id for the parent record.
    pub record_sys_id: String,
}

/// Product-layer view of one readable Task SLA row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSlaView {
    /// Display name of the referenced SLA definition, when readable.
    pub name: Option<String>,
    /// Canonical stage label for known stages or the raw custom label.
    pub stage: Option<String>,
    /// Whether the Task SLA is active.
    pub active: Option<bool>,
    /// Whether the Task SLA has breached.
    pub breached: Option<bool>,
    /// Raw ServiceNow planned end time string.
    pub planned_end_time: Option<String>,
    /// Raw business elapsed percentage. Format through `snow_core::display`.
    pub business_elapsed_percentage: Option<f64>,
    /// Raw ServiceNow `time_left` string. Format through `snow_core::display`.
    pub time_left: Option<String>,
}

/// Summary derived from readable Task SLA rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSlaSummaryView {
    /// Number of readable Task SLA rows.
    pub total: usize,
    /// Number of readable rows where `active == Some(true)`.
    pub active: usize,
    /// Number of readable rows where `breached == Some(true)`.
    pub breached: usize,
    /// Earliest active, unbreached, non-terminal readable row by planned end time.
    pub next_breach: Option<TaskSlaView>,
    /// Highest readable business elapsed percentage.
    pub highest_business_elapsed: Option<f64>,
}

/// Readability state for Task SLA data on a parent record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskSlaReadability {
    /// One or more Task SLA rows were readable.
    ReadableRows,
    /// The crate returned no rows; this may mean no rows or ACL restriction.
    EmptyOrAclRestricted,
    /// The parent record number did not resolve to a readable record.
    ParentNotFound,
    /// The parent table is not Task SLA applicable for this slice.
    NotApplicable,
}

impl SnowCore {
    /// Fetch product-layer Task SLA status for a record number.
    ///
    /// Parent identity and applicability are resolved with `get_by_number`.
    /// Task SLA rows are fetched through the crate's bulk typed helper so the
    /// product layer can preserve `ParentNotFound`, `NotApplicable`, and
    /// `EmptyOrAclRestricted` as distinct outcomes.
    pub async fn task_sla_status_for_number(&self, number: &str) -> Result<TaskSlaStatus> {
        let Some(record) = self.client.get_by_number(number).await? else {
            return Ok(parent_not_found_status(number));
        };
        let parent = parent_ref_from_record(&record, number);

        if !is_task_sla_applicable_table(&parent.record_table) {
            return Ok(status_without_rows(
                parent,
                TaskSlaReadability::NotApplicable,
            ));
        }

        let task_sys_id = parent.record_sys_id.clone();
        let task_ids = [task_sys_id.as_str()];
        let mut by_task = self.client.task_slas_for_tasks(&task_ids).await?;
        let slas = by_task.remove(&task_sys_id).unwrap_or_default();

        Ok(status_from_task_slas(parent, slas))
    }

    /// Fetch product-layer Task SLA status for already-known parent records.
    ///
    /// Task-derived parents are batched into one `task_slas_for_tasks` call.
    /// Non-task-derived parents return `NotApplicable` without issuing an SLA
    /// lookup. The result map is keyed by parent `record_sys_id`.
    pub async fn task_sla_status_for_tasks(
        &self,
        parents: &[TaskSlaParentRef],
    ) -> Result<HashMap<String, TaskSlaStatus>> {
        let task_ids: Vec<&str> = parents
            .iter()
            .filter(|parent| is_task_sla_applicable_table(&parent.record_table))
            .map(|parent| parent.record_sys_id.as_str())
            .collect();

        let by_task = if task_ids.is_empty() {
            HashMap::new()
        } else {
            self.client.task_slas_for_tasks(&task_ids).await?
        };

        let mut statuses = HashMap::with_capacity(parents.len());
        for parent in parents {
            let status = if is_task_sla_applicable_table(&parent.record_table) {
                status_from_task_slas(
                    parent.clone(),
                    by_task
                        .get(parent.record_sys_id.as_str())
                        .cloned()
                        .unwrap_or_default(),
                )
            } else {
                status_without_rows(parent.clone(), TaskSlaReadability::NotApplicable)
            };
            statuses.insert(parent.record_sys_id.clone(), status);
        }

        Ok(statuses)
    }
}

/// Return whether a table is Task SLA applicable for the first consumer slice.
pub fn is_task_sla_applicable_table(table: &str) -> bool {
    let table = table.trim();
    TASK_SLA_APPLICABLE_TABLES
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(table))
}

fn parent_ref_from_record(record: &Record, requested_number: &str) -> TaskSlaParentRef {
    TaskSlaParentRef {
        record_number: record
            .get_str("number")
            .filter(|number| !number.trim().is_empty())
            .unwrap_or(requested_number)
            .to_string(),
        record_table: record.table.clone(),
        record_sys_id: record.sys_id.clone(),
    }
}

fn parent_not_found_status(number: &str) -> TaskSlaStatus {
    TaskSlaStatus {
        record_number: number.to_string(),
        record_table: String::new(),
        record_sys_id: String::new(),
        rows: Vec::new(),
        summary: zero_summary(),
        readable: TaskSlaReadability::ParentNotFound,
    }
}

fn status_without_rows(parent: TaskSlaParentRef, readable: TaskSlaReadability) -> TaskSlaStatus {
    TaskSlaStatus {
        record_number: parent.record_number,
        record_table: parent.record_table,
        record_sys_id: parent.record_sys_id,
        rows: Vec::new(),
        summary: zero_summary(),
        readable,
    }
}

fn status_from_task_slas(parent: TaskSlaParentRef, slas: Vec<TaskSla>) -> TaskSlaStatus {
    if slas.is_empty() {
        return status_without_rows(parent, TaskSlaReadability::EmptyOrAclRestricted);
    }

    let summary = summary_from_task_slas(&slas);
    let rows = slas.iter().map(TaskSlaView::from_task_sla).collect();

    TaskSlaStatus {
        record_number: parent.record_number,
        record_table: parent.record_table,
        record_sys_id: parent.record_sys_id,
        rows,
        summary,
        readable: TaskSlaReadability::ReadableRows,
    }
}

fn summary_from_task_slas(slas: &[TaskSla]) -> TaskSlaSummaryView {
    let summary = TaskSlaSummary::from_task_slas(slas);
    TaskSlaSummaryView {
        total: summary.total,
        active: summary.active,
        breached: summary.breached,
        next_breach: summary.next_breach.as_ref().map(TaskSlaView::from_task_sla),
        highest_business_elapsed: summary.highest_business_elapsed_percentage,
    }
}

fn zero_summary() -> TaskSlaSummaryView {
    TaskSlaSummaryView {
        total: 0,
        active: 0,
        breached: 0,
        next_breach: None,
        highest_business_elapsed: None,
    }
}

impl TaskSlaView {
    fn from_task_sla(sla: &TaskSla) -> Self {
        Self {
            name: sla.sla_name.clone(),
            stage: sla.stage.as_ref().map(stage_label),
            active: sla.active,
            breached: sla.has_breached,
            planned_end_time: sla.planned_end_time.clone(),
            business_elapsed_percentage: sla.business_elapsed_percentage,
            time_left: sla.actual_time_left.clone(),
        }
    }
}

fn stage_label(stage: &TaskSlaStage) -> String {
    match stage {
        TaskSlaStage::InProgress => "in_progress".to_string(),
        TaskSlaStage::Paused => "paused".to_string(),
        TaskSlaStage::Completed => "completed".to_string(),
        TaskSlaStage::Cancelled => "cancelled".to_string(),
        TaskSlaStage::Other(value) => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use servicenow_rs::prelude::{BasicAuth, DisplayValue, ServiceNowClient};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn task_sla_applicability_predicate_matches_first_slice_tables() {
        for table in [
            "task",
            "incident",
            "change_request",
            "change_task",
            "problem",
            "sc_request",
            "sc_req_item",
            "sc_task",
            "rm_story",
            "rm_scrum_task",
        ] {
            assert!(is_task_sla_applicable_table(table), "{table}");
        }

        for table in ["kb_knowledge", "pm_project", "dmn_demand"] {
            assert!(!is_task_sla_applicable_table(table), "{table}");
        }
    }

    #[test]
    fn task_sla_status_maps_synthetic_rows_to_product_view() {
        let parent = TaskSlaParentRef {
            record_number: "INC0000001".to_string(),
            record_table: "incident".to_string(),
            record_sys_id: "parent-sys-1".to_string(),
        };
        let status = status_from_task_slas(
            parent,
            vec![
                synthetic_task_sla(
                    TaskSlaFixture::new("sla-row-1", "parent-sys-1", "Response SLA")
                        .stage("in_progress")
                        .active(true)
                        .breached(false)
                        .planned_end_time("2026-05-08 10:00:00")
                        .business_percentage("80"),
                ),
                synthetic_task_sla(
                    TaskSlaFixture::new("sla-row-2", "parent-sys-1", "Resolution SLA")
                        .stage("completed")
                        .active(true)
                        .breached(true)
                        .planned_end_time("2026-05-08 09:00:00")
                        .business_percentage("95"),
                ),
            ],
        );

        assert_eq!(status.readable, TaskSlaReadability::ReadableRows);
        assert_eq!(status.rows.len(), 2);
        assert_eq!(status.rows[0].name.as_deref(), Some("Response SLA"));
        assert_eq!(status.rows[0].stage.as_deref(), Some("in_progress"));
        assert_eq!(status.rows[0].active, Some(true));
        assert_eq!(status.rows[0].breached, Some(false));
        assert_eq!(status.rows[0].business_elapsed_percentage, Some(80.0));
        assert_eq!(
            status.rows[0].time_left.as_deref(),
            Some("1970-01-01 04:00:00")
        );
        assert_eq!(status.summary.total, 2);
        assert_eq!(status.summary.active, 2);
        assert_eq!(status.summary.breached, 1);
        assert_eq!(status.summary.highest_business_elapsed, Some(95.0));
        assert_eq!(
            status
                .summary
                .next_breach
                .as_ref()
                .and_then(|row| row.name.as_deref()),
            Some("Response SLA")
        );
    }

    #[test]
    fn task_sla_status_readability_variants_are_distinct() {
        let parent = TaskSlaParentRef {
            record_number: "INC0000001".to_string(),
            record_table: "incident".to_string(),
            record_sys_id: "parent-sys-1".to_string(),
        };

        let empty = status_from_task_slas(parent.clone(), Vec::new());
        assert_eq!(empty.readable, TaskSlaReadability::EmptyOrAclRestricted);
        assert_eq!(empty.summary.total, 0);

        let not_applicable = status_without_rows(parent.clone(), TaskSlaReadability::NotApplicable);
        assert_eq!(not_applicable.readable, TaskSlaReadability::NotApplicable);
        assert_eq!(not_applicable.record_sys_id, "parent-sys-1");

        let missing = parent_not_found_status("INC0000009");
        assert_eq!(missing.readable, TaskSlaReadability::ParentNotFound);
        assert_eq!(missing.record_number, "INC0000009");
        assert!(missing.record_table.is_empty());
        assert!(missing.record_sys_id.is_empty());
    }

    #[tokio::test]
    async fn task_sla_status_for_number_uses_parent_lookup_and_bulk_sla_helper() {
        let server = MockServer::start().await;
        mount_parent_record(
            &server,
            "incident",
            json!({
                "sys_id": "parent-sys-1",
                "number": "INC0000001",
                "short_description": "Generic incident"
            }),
        )
        .await;
        mount_task_sla_rows(
            &server,
            vec![task_sla_row(
                TaskSlaFixture::new("sla-row-1", "parent-sys-1", "Response SLA")
                    .task_display("INC0000001")
                    .stage("in_progress")
                    .active(true)
                    .breached(false)
                    .planned_end_time("2026-05-08 10:00:00")
                    .business_percentage("72.5"),
            )],
        )
        .await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(&server, tempdir.path().join("vault")).await;
        let status = core
            .task_sla_status_for_number("INC0000001")
            .await
            .expect("status");

        assert_eq!(status.record_number, "INC0000001");
        assert_eq!(status.record_table, "incident");
        assert_eq!(status.record_sys_id, "parent-sys-1");
        assert_eq!(status.readable, TaskSlaReadability::ReadableRows);
        assert_eq!(status.rows.len(), 1);
        assert_eq!(status.summary.total, 1);

        let task_sla_requests = requests_for_path(&server, "/api/now/table/task_sla").await;
        assert_eq!(task_sla_requests.len(), 1);
    }

    #[tokio::test]
    async fn task_sla_status_for_number_preserves_empty_or_acl_restricted() {
        let server = MockServer::start().await;
        mount_parent_record(
            &server,
            "incident",
            json!({
                "sys_id": "parent-sys-empty",
                "number": "INC0000002",
                "short_description": "Generic incident"
            }),
        )
        .await;
        mount_task_sla_rows(&server, Vec::new()).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(&server, tempdir.path().join("vault")).await;
        let status = core
            .task_sla_status_for_number("INC0000002")
            .await
            .expect("status");

        assert_eq!(status.readable, TaskSlaReadability::EmptyOrAclRestricted);
        assert_eq!(status.summary.total, 0);
        assert!(status.rows.is_empty());
    }

    #[tokio::test]
    async fn task_sla_status_for_number_returns_parent_not_found_without_sla_fetch() {
        let server = MockServer::start().await;
        mount_parent_records(&server, "incident", Vec::new()).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(&server, tempdir.path().join("vault")).await;
        let status = core
            .task_sla_status_for_number("INC0000009")
            .await
            .expect("status");

        assert_eq!(status.readable, TaskSlaReadability::ParentNotFound);
        assert_eq!(status.record_number, "INC0000009");
        assert!(status.record_table.is_empty());
        assert!(status.record_sys_id.is_empty());
        assert!(
            requests_for_path(&server, "/api/now/table/task_sla")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn task_sla_status_for_number_returns_not_applicable_without_sla_fetch() {
        let server = MockServer::start().await;
        mount_parent_record(
            &server,
            "kb_knowledge",
            json!({
                "sys_id": "kb-sys-1",
                "number": "KB0000001",
                "short_description": "Generic knowledge article"
            }),
        )
        .await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(&server, tempdir.path().join("vault")).await;
        let status = core
            .task_sla_status_for_number("KB0000001")
            .await
            .expect("status");

        assert_eq!(status.readable, TaskSlaReadability::NotApplicable);
        assert_eq!(status.record_table, "kb_knowledge");
        assert_eq!(status.record_sys_id, "kb-sys-1");
        assert!(
            requests_for_path(&server, "/api/now/table/task_sla")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn task_sla_status_for_tasks_batches_task_derived_parents_only() {
        let server = MockServer::start().await;
        mount_task_sla_rows(
            &server,
            vec![task_sla_row(
                TaskSlaFixture::new("sla-row-1", "parent-sys-1", "Response SLA")
                    .task_display("INC0000001")
                    .stage("in_progress")
                    .active(true)
                    .breached(false)
                    .planned_end_time("2026-05-08 10:00:00")
                    .business_percentage("72.5"),
            )],
        )
        .await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(&server, tempdir.path().join("vault")).await;
        let statuses = core
            .task_sla_status_for_tasks(&[
                TaskSlaParentRef {
                    record_number: "INC0000001".to_string(),
                    record_table: "incident".to_string(),
                    record_sys_id: "parent-sys-1".to_string(),
                },
                TaskSlaParentRef {
                    record_number: "KB0000001".to_string(),
                    record_table: "kb_knowledge".to_string(),
                    record_sys_id: "kb-sys-1".to_string(),
                },
            ])
            .await
            .expect("statuses");

        assert_eq!(
            statuses
                .get("parent-sys-1")
                .expect("incident status")
                .readable,
            TaskSlaReadability::ReadableRows
        );
        assert_eq!(
            statuses.get("kb-sys-1").expect("kb status").readable,
            TaskSlaReadability::NotApplicable
        );

        let task_sla_requests = requests_for_path(&server, "/api/now/table/task_sla").await;
        assert_eq!(task_sla_requests.len(), 1);
    }

    struct TaskSlaFixture<'a> {
        sys_id: &'a str,
        task_sys_id: &'a str,
        task_display: &'a str,
        sla_display: &'a str,
        stage: &'a str,
        active: bool,
        has_breached: bool,
        planned_end_time: &'a str,
        business_percentage: &'a str,
    }

    impl<'a> TaskSlaFixture<'a> {
        fn new(sys_id: &'a str, task_sys_id: &'a str, sla_display: &'a str) -> Self {
            Self {
                sys_id,
                task_sys_id,
                task_display: "INC0000001",
                sla_display,
                stage: "in_progress",
                active: true,
                has_breached: false,
                planned_end_time: "2026-05-08 10:00:00",
                business_percentage: "0",
            }
        }

        fn task_display(mut self, value: &'a str) -> Self {
            self.task_display = value;
            self
        }

        fn stage(mut self, value: &'a str) -> Self {
            self.stage = value;
            self
        }

        fn active(mut self, value: bool) -> Self {
            self.active = value;
            self
        }

        fn breached(mut self, value: bool) -> Self {
            self.has_breached = value;
            self
        }

        fn planned_end_time(mut self, value: &'a str) -> Self {
            self.planned_end_time = value;
            self
        }

        fn business_percentage(mut self, value: &'a str) -> Self {
            self.business_percentage = value;
            self
        }
    }

    fn synthetic_task_sla(fixture: TaskSlaFixture<'_>) -> TaskSla {
        let record = Record::from_json("task_sla", &task_sla_row(fixture), DisplayValue::Both)
            .expect("task_sla record");
        TaskSla::from_record(record)
    }

    fn task_sla_row(fixture: TaskSlaFixture<'_>) -> Value {
        json!({
            "sys_id": { "value": fixture.sys_id, "display_value": fixture.sys_id },
            "task": { "value": fixture.task_sys_id, "display_value": fixture.task_display },
            "sla": {
                "value": format!("{}-definition", fixture.sys_id),
                "display_value": fixture.sla_display
            },
            "stage": { "value": fixture.stage, "display_value": fixture.stage },
            "active": {
                "value": fixture.active.to_string(),
                "display_value": fixture.active.to_string()
            },
            "has_breached": {
                "value": fixture.has_breached.to_string(),
                "display_value": fixture.has_breached.to_string()
            },
            "planned_end_time": {
                "value": fixture.planned_end_time,
                "display_value": fixture.planned_end_time
            },
            "business_percentage": {
                "value": fixture.business_percentage,
                "display_value": fixture.business_percentage
            },
            "time_left": {
                "value": "1970-01-01 04:00:00",
                "display_value": "4 Hours"
            }
        })
    }

    async fn mount_parent_record(server: &MockServer, table: &str, record: Value) {
        mount_parent_records(server, table, vec![record]).await;
    }

    async fn mount_parent_records(server: &MockServer, table: &str, records: Vec<Value>) {
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/{table}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": records
            })))
            .mount(server)
            .await;
    }

    async fn mount_task_sla_rows(server: &MockServer, rows: Vec<Value>) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/task_sla"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": rows
            })))
            .mount(server)
            .await;
    }

    async fn build_test_core(
        server: &MockServer,
        vault_path: impl Into<std::path::PathBuf>,
    ) -> SnowCore {
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        SnowCore::builder()
            .client(client)
            .vault_path(vault_path)
            .build()
            .await
            .expect("core")
    }

    async fn requests_for_path(server: &MockServer, api_path: &str) -> Vec<wiremock::Request> {
        server
            .received_requests()
            .await
            .expect("request recording enabled")
            .into_iter()
            .filter(|request| request.url.path() == api_path)
            .collect()
    }
}
