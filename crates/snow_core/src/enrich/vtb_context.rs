//! Visual Task Board private-task context enrichment.
//!
//! Table/field names frozen from Gate 0 live probes (2026-07-16). This
//! module resolves the board/lane a `vtb_task` (PTSK) card is sitting on and
//! its checklist, so the CLI can render a Kanban-ish summary next to the
//! base task fields. Every fetch here is best-effort: ServiceNow ACLs on
//! `vtb_*`/`checklist*` tables vary per instance/role, so a locked-down role
//! degrades to a plainer private-task view instead of failing the whole
//! `show` command.

use anyhow::Result;
use serde::Serialize;
use servicenow_rs::prelude::{DisplayValue, Order, ServiceNowClient};

use crate::helpers::{non_empty_owned, parse_i64, record_bool, servicenow_reference_sys_id};

/// Polymorphic discriminator ServiceNow stores in `checklist.table` for
/// checklists attached to private tasks (Gate 0). Not part of [`VtbSchema`]
/// because it names a filter *value*, not a table/field, and it is only
/// ever used to scope the `checklist` document lookup below.
const CHECKLIST_DOCUMENT_TABLE: &str = "vtb_task";

/// Safety cap on checklist items fetched per task. Checklists on a single
/// card are a human-curated list, not a bulk table — this is generous
/// headroom, not a real-world expectation.
const CHECKLIST_ITEM_FETCH_LIMIT: u32 = 200;

/// Live-verified table/field names (Gate 0, 2026-07-16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VtbSchema {
    pub card_table: &'static str,
    pub card_task_field: &'static str,
    pub card_board_field: &'static str,
    pub card_lane_field: &'static str,
    pub card_swim_lane_field: &'static str,
    pub card_order_field: &'static str,
    pub board_table: &'static str,
    pub board_name_field: &'static str,
    pub lane_table: &'static str,
    pub lane_name_field: &'static str,
    pub checklist_table: &'static str,
    pub checklist_document_field: &'static str,
    pub checklist_table_field: &'static str,
    pub checklist_item_table: &'static str,
    pub checklist_item_parent_field: &'static str,
    pub checklist_item_name_field: &'static str,
    pub checklist_item_complete_field: &'static str,
    pub checklist_item_order_field: &'static str,
}

impl VtbSchema {
    /// Constants confirmed against the operator instance meta/table probes.
    pub const GATE0: Self = Self {
        card_table: "vtb_card",
        card_task_field: "task",
        card_board_field: "board",
        card_lane_field: "lane",
        card_swim_lane_field: "swim_lane",
        card_order_field: "order",
        board_table: "vtb_board",
        board_name_field: "name",
        lane_table: "vtb_lane",
        lane_name_field: "name",
        checklist_table: "checklist",
        checklist_document_field: "document",
        checklist_table_field: "table",
        checklist_item_table: "checklist_item",
        checklist_item_parent_field: "checklist",
        checklist_item_name_field: "name",
        checklist_item_complete_field: "complete",
        checklist_item_order_field: "order",
    };
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct VtbContext {
    pub board_sys_id: Option<String>,
    pub board_name: Option<String>,
    pub lane_sys_id: Option<String>,
    pub lane_name: Option<String>,
    pub swim_lane_sys_id: Option<String>,
    pub swim_lane_name: Option<String>,
    pub card_order: Option<i64>,
    pub checklist_items: Vec<VtbChecklistItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VtbChecklistItem {
    pub sys_id: String,
    pub name: String,
    pub complete: bool,
    pub order: Option<i64>,
}

/// Fetch board/lane/checklist context for a private task (`vtb_task`).
///
/// Infallible contract: this function returns `VtbContext` directly, never
/// `Err` — there is no failure path a caller needs to handle. `vtb_card`
/// lookup (which now also resolves board/lane/swim_lane names via a single
/// dot-walked query, see [`fetch_card_for_task`]) and checklist lookup are
/// each caught independently — a checklist ACL failure does not blank out
/// an already-resolved board/lane, and a missing/unreadable card just means
/// board/lane stay `None` while the checklist fetch still runs (it does not
/// depend on the card). Each skipped step, including a dangling reference
/// name surfaced by the dot-walked query, is logged via `tracing::debug!`,
/// mirroring how [`crate::context::CoreContext::enrich_record_journals`]
/// treats journal enrichment as best-effort and logs-and-continues rather
/// than failing the parent `get_record`. Callers (e.g. `snow_cli`'s
/// `cmd_show_private_task`) can call this unconditionally without `?` —
/// there is nothing to propagate.
pub async fn enrich_vtb_context(
    client: &ServiceNowClient,
    schema: &VtbSchema,
    task_sys_id: &str,
) -> VtbContext {
    let (card, checklist_items) = tokio::join!(
        fetch_card_for_task(client, schema, task_sys_id),
        fetch_checklist_items(client, schema, task_sys_id)
    );
    let mut context = VtbContext::default();

    match card {
        Ok(Some(card)) => {
            context.card_order = card.order;
            context.board_sys_id = card.board_sys_id;
            context.board_name = card.board_name;
            context.lane_sys_id = card.lane_sys_id;
            context.lane_name = card.lane_name;
            context.swim_lane_sys_id = card.swim_lane_sys_id;
            context.swim_lane_name = card.swim_lane_name;
        }
        Ok(None) => {
            tracing::debug!(
                task_sys_id = %task_sys_id,
                "no vtb_card row for private task; board/lane left unset"
            );
        }
        Err(err) => {
            tracing::debug!(
                task_sys_id = %task_sys_id,
                error = %err,
                "vtb_card lookup failed; degrading to no board/lane context"
            );
        }
    }

    context.checklist_items = match checklist_items {
        Ok(items) => items,
        Err(err) => {
            tracing::debug!(
                task_sys_id = %task_sys_id,
                error = %err,
                "checklist lookup failed; degrading to empty checklist"
            );
            Vec::new()
        }
    };

    context
}

/// `vtb_card` row for a task sys_id (`task=<sys_id>`), with board/lane/
/// swim_lane display names resolved inline via dot-walking.
///
/// One card per task is assumed — Gate 0 did not surface duplicate-card
/// cases, and `vtb_card` is meant to be a task's single position on a
/// single board. Returns `Ok(None)` when the task has no card yet (normal:
/// a `vtb_task` can exist before it's dropped onto a board), not a failure.
/// Propagates the underlying `Err` on transport/ACL failure — callers that
/// want the best-effort degrade (see [`enrich_vtb_context`]) are
/// responsible for catching it.
///
/// Name resolution: rather than one follow-up `sys_id=<sys_id>` query per
/// reference field (up to 3 extra round trips), the reference names are
/// pulled inline on this single request via `dot_walk` (see
/// `servicenow_rs::query::builder::QueryBuilder::dot_walk`, e.g.
/// `client.table("incident").dot_walk(&["assigned_to.name"])`). The
/// dot-walked field names are built from [`VtbSchema`] rather than
/// hardcoded literals — `format!("{}.{}", schema.card_board_field,
/// schema.board_name_field)` and the same shape for lane/swim_lane — so a
/// schema change (e.g. a future Gate re-probe renaming `board` or `name`)
/// only touches [`VtbSchema::GATE0`], not this query. `swim_lane` dot-walks
/// through `schema.lane_name_field` because Gate 0 confirmed swim lanes
/// live in the same `vtb_lane` table as regular lanes.
///
/// `.fields(...)` is restricted to what this function and its caller
/// actually consume (`sys_id`, the task/board/lane/swim_lane/order columns)
/// — the dot-walked name fields are appended to `sysparm_fields`
/// automatically by the query builder, so they don't need to be repeated
/// here. This mirrors [`fetch_checklist_items`]'s existing field
/// restriction on `checklist_item`.
pub async fn fetch_card_for_task(
    client: &ServiceNowClient,
    schema: &VtbSchema,
    task_sys_id: &str,
) -> Result<Option<VtbCardRow>> {
    let board_name_field = format!("{}.{}", schema.card_board_field, schema.board_name_field);
    let lane_name_field = format!("{}.{}", schema.card_lane_field, schema.lane_name_field);
    let swim_lane_name_field =
        format!("{}.{}", schema.card_swim_lane_field, schema.lane_name_field);

    let record = client
        .table(schema.card_table)
        .equals(schema.card_task_field, task_sys_id)
        .fields(&[
            "sys_id",
            schema.card_task_field,
            schema.card_board_field,
            schema.card_lane_field,
            schema.card_swim_lane_field,
            schema.card_order_field,
        ])
        .dot_walk(&[
            board_name_field.as_str(),
            lane_name_field.as_str(),
            swim_lane_name_field.as_str(),
        ])
        .display_value(DisplayValue::Raw)
        .first()
        .await?;

    Ok(record.map(|record| {
        let board_sys_id = servicenow_reference_sys_id(&record, schema.card_board_field);
        let lane_sys_id = servicenow_reference_sys_id(&record, schema.card_lane_field);
        let swim_lane_sys_id = servicenow_reference_sys_id(&record, schema.card_swim_lane_field);

        let board_name = resolve_dot_walked_name(
            board_sys_id.as_deref(),
            record.get_str(&board_name_field),
            task_sys_id,
            "board",
        );
        let lane_name = resolve_dot_walked_name(
            lane_sys_id.as_deref(),
            record.get_str(&lane_name_field),
            task_sys_id,
            "lane",
        );
        let swim_lane_name = resolve_dot_walked_name(
            swim_lane_sys_id.as_deref(),
            record.get_str(&swim_lane_name_field),
            task_sys_id,
            "swim_lane",
        );

        VtbCardRow {
            board_sys_id,
            board_name,
            lane_sys_id,
            lane_name,
            swim_lane_sys_id,
            swim_lane_name,
            order: parse_i64(record.get_str(schema.card_order_field)),
        }
    }))
}

/// Card row shape used by enrichment: reference sys_ids alongside the
/// display names dot-walked off the same `vtb_card` query (see
/// [`fetch_card_for_task`]). Keeping both lets [`enrich_vtb_context`] copy
/// straight across to [`VtbContext`] without a second resolution pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VtbCardRow {
    pub board_sys_id: Option<String>,
    pub board_name: Option<String>,
    pub lane_sys_id: Option<String>,
    pub lane_name: Option<String>,
    pub swim_lane_sys_id: Option<String>,
    pub swim_lane_name: Option<String>,
    pub order: Option<i64>,
}

/// Checklist items for a private task, in board order.
///
/// Two-step query per Gate 0: `checklist` rows are polymorphic
/// (`document` + `table`), so the first hop resolves the one `checklist`
/// row scoped to this task (`document=<task_sys_id>` AND
/// `table=vtb_task`), then the second hop lists `checklist_item` rows for
/// that checklist ordered by `order` ascending. Returns `Ok(Vec::new())`
/// both when the task has no checklist yet and when the checklist has no
/// items. Propagates `Err` on transport/ACL failure — see
/// [`enrich_vtb_context`] for the best-effort wrapper.
pub async fn fetch_checklist_items(
    client: &ServiceNowClient,
    schema: &VtbSchema,
    task_sys_id: &str,
) -> Result<Vec<VtbChecklistItem>> {
    let checklist = client
        .table(schema.checklist_table)
        .equals(schema.checklist_document_field, task_sys_id)
        .equals(schema.checklist_table_field, CHECKLIST_DOCUMENT_TABLE)
        .display_value(DisplayValue::Raw)
        .first()
        .await?;
    let Some(checklist) = checklist else {
        return Ok(Vec::new());
    };

    let items = client
        .table(schema.checklist_item_table)
        .equals(schema.checklist_item_parent_field, &checklist.sys_id)
        .fields(&[
            "sys_id",
            schema.checklist_item_name_field,
            schema.checklist_item_complete_field,
            schema.checklist_item_order_field,
        ])
        .order_by(schema.checklist_item_order_field, Order::Asc)
        .display_value(DisplayValue::Raw)
        .limit(CHECKLIST_ITEM_FETCH_LIMIT)
        .execute()
        .await?;

    Ok(items
        .records
        .into_iter()
        .map(|record| VtbChecklistItem {
            sys_id: record.sys_id.clone(),
            name: record
                .get_str(schema.checklist_item_name_field)
                .unwrap_or_default()
                .to_string(),
            complete: record_bool(&record, schema.checklist_item_complete_field),
            order: parse_i64(record.get_str(schema.checklist_item_order_field)),
        })
        .collect())
}

/// Extract a dot-walked reference name from a `vtb_card` row, treating a
/// set `sys_id` with a missing/empty dot-walked name as a logged
/// degradation rather than a silent `None`.
///
/// This covers the two ways a name can legitimately be absent: the
/// reference field itself is unset (`sys_id` is `None` — nothing to log,
/// there's no reference to resolve), or the reference is set but the
/// dot-walked value came back missing/empty (a dangling reference, or the
/// name field trimmed by an ACL) — that case is logged via
/// `tracing::debug!`, grouped with the other best-effort degradations this
/// module logs (missing card, checklist ACL failure) per the module's
/// contract of treating "missing rows" the same as "ACL/timeout" noise.
fn resolve_dot_walked_name(
    sys_id: Option<&str>,
    dot_walked_value: Option<&str>,
    task_sys_id: &str,
    role: &str,
) -> Option<String> {
    let name = non_empty_owned(dot_walked_value);
    if sys_id.is_some() && name.is_none() {
        tracing::debug!(
            task_sys_id = %task_sys_id,
            sys_id = %sys_id.unwrap_or_default(),
            role = %role,
            "vtb reference name missing from dot-walked vtb_card query; leaving name unset"
        );
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use servicenow_rs::prelude::BasicAuth;
    use wiremock::matchers::{method, path, query_param_contains};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn gate0_schema_constants_match_live_probe_names() {
        let s = VtbSchema::GATE0;
        assert_eq!(s.card_table, "vtb_card");
        assert_eq!(s.card_task_field, "task");
        assert_eq!(s.card_board_field, "board");
        assert_eq!(s.card_lane_field, "lane");
        assert_eq!(s.card_swim_lane_field, "swim_lane");
        assert_eq!(s.checklist_table, "checklist");
        assert_eq!(s.checklist_document_field, "document");
        assert_eq!(s.checklist_table_field, "table");
        assert_eq!(s.checklist_item_parent_field, "checklist");
        assert_eq!(s.checklist_item_complete_field, "complete");
    }

    // Placeholder sys_ids: 32-hex-char strings built from a single repeated
    // digit per role, kept distinct so mock matching can't cross-hit. Never
    // real ServiceNow sys_ids — see the repo's pre-push sensitive-data scan.
    const TASK_SYS_ID: &str = "11111111111111111111111111111111";
    const CARD_SYS_ID: &str = "22222222222222222222222222222222";
    const BOARD_SYS_ID: &str = "33333333333333333333333333333333";
    const LANE_SYS_ID: &str = "44444444444444444444444444444444";
    const SWIM_LANE_SYS_ID: &str = "55555555555555555555555555555555";
    const CHECKLIST_SYS_ID: &str = "66666666666666666666666666666666";
    const ITEM_A_SYS_ID: &str = "77777777777777777777777777777777";
    const ITEM_B_SYS_ID: &str = "88888888888888888888888888888888";

    async fn test_client(server: &MockServer) -> ServiceNowClient {
        ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client")
    }

    /// Build a `vtb_card` row shaped like what the dot-walked query now
    /// asks for: sys_id fields plus flat `"<field>.name"` keys, matching
    /// how ServiceNow returns dot-walked fields (see
    /// `servicenow_rs::model::record::Record::get_str` doc — dot-walked
    /// values arrive as flat keys, not nested objects).
    fn card_row(
        board: &str,
        board_name: &str,
        lane: &str,
        lane_name: &str,
        swim_lane: Option<(&str, &str)>,
        order: &str,
    ) -> Value {
        let (swim_lane_sys_id, swim_lane_name) = swim_lane.unwrap_or(("", ""));
        json!({
            "sys_id": CARD_SYS_ID,
            "task": TASK_SYS_ID,
            "board": board,
            "board.name": board_name,
            "lane": lane,
            "lane.name": lane_name,
            "swim_lane": swim_lane_sys_id,
            "swim_lane.name": swim_lane_name,
            "order": order,
        })
    }

    async fn mount_card(server: &MockServer, rows: Vec<Value>) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/vtb_card"))
            .and(query_param_contains(
                "sysparm_query",
                format!("task={TASK_SYS_ID}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(server)
            .await;
    }

    async fn mount_checklist(server: &MockServer, rows: Vec<Value>) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/checklist"))
            .and(query_param_contains(
                "sysparm_query",
                format!("document={TASK_SYS_ID}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(server)
            .await;
    }

    async fn mount_checklist_forbidden(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/checklist"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "error": { "message": "ACL restriction", "detail": null }
            })))
            .mount(server)
            .await;
    }

    async fn mount_checklist_items(server: &MockServer, rows: Vec<Value>) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/checklist_item"))
            .and(query_param_contains(
                "sysparm_query",
                format!("checklist={CHECKLIST_SYS_ID}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": rows })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn enrich_vtb_context_happy_path_resolves_board_lane_and_checklist() {
        let server = MockServer::start().await;
        mount_card(
            &server,
            vec![card_row(
                BOARD_SYS_ID,
                "Example Board",
                LANE_SYS_ID,
                "In Progress",
                Some((SWIM_LANE_SYS_ID, "Escalations")),
                "100",
            )],
        )
        .await;
        mount_checklist(
            &server,
            vec![json!({
                "sys_id": CHECKLIST_SYS_ID,
                "document": TASK_SYS_ID,
                "table": "vtb_task",
            })],
        )
        .await;
        mount_checklist_items(
            &server,
            vec![
                json!({
                    "sys_id": ITEM_A_SYS_ID,
                    "checklist": CHECKLIST_SYS_ID,
                    "name": "Confirm scope",
                    "complete": "true",
                    "order": "1",
                }),
                json!({
                    "sys_id": ITEM_B_SYS_ID,
                    "checklist": CHECKLIST_SYS_ID,
                    "name": "Notify requester",
                    "complete": "false",
                    "order": "2",
                }),
            ],
        )
        .await;

        let client = test_client(&server).await;
        let context = enrich_vtb_context(&client, &VtbSchema::GATE0, TASK_SYS_ID).await;

        assert_eq!(context.board_sys_id.as_deref(), Some(BOARD_SYS_ID));
        assert_eq!(context.board_name.as_deref(), Some("Example Board"));
        assert_eq!(context.lane_sys_id.as_deref(), Some(LANE_SYS_ID));
        assert_eq!(context.lane_name.as_deref(), Some("In Progress"));
        assert_eq!(context.swim_lane_sys_id.as_deref(), Some(SWIM_LANE_SYS_ID));
        assert_eq!(context.swim_lane_name.as_deref(), Some("Escalations"));
        assert_eq!(context.card_order, Some(100));

        assert_eq!(context.checklist_items.len(), 2);
        assert_eq!(context.checklist_items[0].sys_id, ITEM_A_SYS_ID);
        assert_eq!(context.checklist_items[0].name, "Confirm scope");
        assert!(context.checklist_items[0].complete);
        assert_eq!(context.checklist_items[0].order, Some(1));
        assert_eq!(context.checklist_items[1].sys_id, ITEM_B_SYS_ID);
        assert_eq!(context.checklist_items[1].name, "Notify requester");
        assert!(!context.checklist_items[1].complete);
        assert_eq!(context.checklist_items[1].order, Some(2));
    }

    #[tokio::test]
    async fn enrich_vtb_context_without_card_returns_default_context() {
        let server = MockServer::start().await;
        mount_card(&server, Vec::new()).await;
        mount_checklist(&server, Vec::new()).await;

        let client = test_client(&server).await;
        let context = enrich_vtb_context(&client, &VtbSchema::GATE0, TASK_SYS_ID).await;

        assert_eq!(context, VtbContext::default());
    }

    #[tokio::test]
    async fn enrich_vtb_context_checklist_403_degrades_to_empty_checklist_but_keeps_board_lane() {
        let server = MockServer::start().await;
        mount_card(
            &server,
            vec![card_row(
                BOARD_SYS_ID,
                "Example Board",
                LANE_SYS_ID,
                "In Progress",
                None,
                "5",
            )],
        )
        .await;
        mount_checklist_forbidden(&server).await;

        let client = test_client(&server).await;
        let context = enrich_vtb_context(&client, &VtbSchema::GATE0, TASK_SYS_ID).await;

        assert_eq!(context.board_name.as_deref(), Some("Example Board"));
        assert_eq!(context.lane_name.as_deref(), Some("In Progress"));
        assert!(context.swim_lane_sys_id.is_none());
        assert!(context.checklist_items.is_empty());
    }

    #[tokio::test]
    async fn fetch_card_for_task_returns_none_without_a_card() {
        let server = MockServer::start().await;
        mount_card(&server, Vec::new()).await;

        let client = test_client(&server).await;
        let card = fetch_card_for_task(&client, &VtbSchema::GATE0, TASK_SYS_ID)
            .await
            .expect("query succeeds");

        assert!(card.is_none());
    }

    #[tokio::test]
    async fn fetch_card_for_task_requests_dot_walked_name_fields_and_restricted_field_list() {
        let server = MockServer::start().await;
        // Finding 1 + 2: the card query must dot-walk board/lane/swim_lane
        // names on the single request (no separate per-role HTTP calls),
        // and `sysparm_fields` must be restricted to what's consumed rather
        // than an unrestricted `fields()`-less query.
        Mock::given(method("GET"))
            .and(path("/api/now/table/vtb_card"))
            .and(query_param_contains(
                "sysparm_query",
                format!("task={TASK_SYS_ID}"),
            ))
            .and(query_param_contains("sysparm_fields", "board.name"))
            .and(query_param_contains("sysparm_fields", "lane.name"))
            .and(query_param_contains("sysparm_fields", "swim_lane.name"))
            .and(query_param_contains("sysparm_fields", "sys_id"))
            .and(query_param_contains("sysparm_fields", "board"))
            .and(query_param_contains("sysparm_fields", "order"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [card_row(
                    BOARD_SYS_ID,
                    "Example Board",
                    LANE_SYS_ID,
                    "In Progress",
                    Some((SWIM_LANE_SYS_ID, "Escalations")),
                    "100",
                )]
            })))
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let card = fetch_card_for_task(&client, &VtbSchema::GATE0, TASK_SYS_ID)
            .await
            .expect("query succeeds")
            .expect("card present");

        // The mock only matches (and thus only responds) when the request
        // carries the expected sysparm_fields/dot-walk shape above, so a
        // successful, correctly-populated result here proves the request
        // shape, not just the response parsing.
        assert_eq!(card.board_sys_id.as_deref(), Some(BOARD_SYS_ID));
        assert_eq!(card.board_name.as_deref(), Some("Example Board"));
        assert_eq!(card.lane_sys_id.as_deref(), Some(LANE_SYS_ID));
        assert_eq!(card.lane_name.as_deref(), Some("In Progress"));
        assert_eq!(card.swim_lane_sys_id.as_deref(), Some(SWIM_LANE_SYS_ID));
        assert_eq!(card.swim_lane_name.as_deref(), Some("Escalations"));
        assert_eq!(card.order, Some(100));
    }

    #[tokio::test]
    async fn fetch_card_for_task_dangling_board_reference_leaves_name_unset() {
        // Finding 3: a set reference (sys_id present) whose dot-walked name
        // comes back missing/empty is a logged degradation, not a silent
        // `None` indistinguishable from "no board set".
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/vtb_card"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [card_row(BOARD_SYS_ID, "", LANE_SYS_ID, "In Progress", None, "1")]
            })))
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let card = fetch_card_for_task(&client, &VtbSchema::GATE0, TASK_SYS_ID)
            .await
            .expect("query succeeds")
            .expect("card present");

        assert_eq!(card.board_sys_id.as_deref(), Some(BOARD_SYS_ID));
        assert!(
            card.board_name.is_none(),
            "dangling board reference should leave name unset rather than surfacing empty string"
        );
        assert_eq!(card.lane_name.as_deref(), Some("In Progress"));
    }

    #[tokio::test]
    async fn fetch_checklist_items_returns_empty_without_a_checklist_row() {
        let server = MockServer::start().await;
        mount_checklist(&server, Vec::new()).await;

        let client = test_client(&server).await;
        let items = fetch_checklist_items(&client, &VtbSchema::GATE0, TASK_SYS_ID)
            .await
            .expect("query succeeds");

        assert!(items.is_empty());
    }
}
