//! L0 cross-repository consumer proof. Only the HTTP provider is faked; Mullet,
//! framed daemon transport, governance, core clients, and durable receipts run.
use crate::{DaemonState, test_support::build_fixture_state_with_ui_metadata};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const USER: &str = "11111111111111111111111111111111";
const GROUP: &str = "22222222222222222222222222222222";
const SPRINT: &str = "33333333333333333333333333333333";
const CARD: &str = "77777777777777777777777777777777";

fn record(table: &str, id: &str, number: &str) -> Value {
    json!({"sys_id":id,"number":number,"sys_class_name":table,"short_description":format!("Example {table}"),
        "description":"Example record body","state":"1","active":"true","sys_updated_on":"2026-09-01 12:00:00","sys_mod_count":"1",
        "assignment_group":GROUP,"assigned_to":USER,"sprint":SPRINT,"backlog_type":"product","percent_complete":"0","work_notes":"","comments":""})
}

#[derive(Default)]
struct Provider {
    rows: BTreeMap<String, Vec<Value>>,
    writes: Vec<(String, Value)>,
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "cross-repository release gate; run Mullet test:servicenow-e2e"]
async fn daemon_surfaces_end_to_end_via_mullet_stdio() {
    let instance = MockServer::start().await;
    let mut provider = Provider::default();
    for (table, id, number) in [
        ("change_request", "chg-sys", "CHG001"),
        ("change_task", "ctask-sys", "CTASK001"),
        ("incident", "inc-sys", "INC001"),
        ("rm_story", "story-sys", "STRY001"),
        (
            "resource_plan",
            "88888888888888888888888888888888",
            "RPLN001",
        ),
    ] {
        provider
            .rows
            .insert(table.into(), vec![record(table, id, number)]);
    }
    provider.rows.get_mut("resource_plan").unwrap()[0]["state"] = json!("2");
    provider.rows.insert("sys_user".into(),vec![json!({"sys_id":USER,"user_name":"tester","name":"Example User","email":"tester@example.com","active":"true"})]);
    provider.rows.insert("kb_knowledge".into(), vec![]);
    provider.rows.insert("rm_scrum_task".into(), vec![]);
    provider.rows.insert("resource_allocation".into(),vec![json!({"sys_id":"allocation-sys","resource_plan":"88888888888888888888888888888888","booking_type":{"value":"1","display_value":"Soft"}})]);
    provider.rows.insert("time_card".into(),vec![json!({"sys_id":CARD,"user":{"value":USER,"display_value":"Example User"},"user.user_name":"tester","user.email":"tester@example.com","time_sheet":{"value":"sheet-sys","display_value":"2026-08-30"},"week_starts_on":"2026-08-30","category":"project_work","state":"Pending","task":{"value":"story-sys","display_value":"STRY001"},"task.number":"STRY001","task.sys_class_name":"rm_story","sunday":"0","monday":"0","tuesday":"0","wednesday":"0","thursday":"0","friday":"0","saturday":"0","total":"0","sys_updated_on":"2026-09-01 12:00:00","sys_mod_count":"1"})]);
    let provider = Arc::new(Mutex::new(provider));
    let http_state = Arc::clone(&provider);
    Mock::given(wiremock::matchers::any())
        .respond_with(move |request: &wiremock::Request| {
            let Some(rest) = request.url.path().strip_prefix("/api/now/table/") else {
                return ResponseTemplate::new(404);
            };
            let (table, id) = rest
                .split_once('/')
                .map_or((rest, None), |(table, id)| (table, Some(id)));
            let query = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "sysparm_query")
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default();
            let mut state = http_state.lock().unwrap();
            if request.method == "GET" {
                if ["sys_dictionary", "sys_db_object", "sys_choice"].contains(&table) {
                    return ResponseTemplate::new(200).set_body_json(json!({"result":[]}));
                }
                let Some(rows) = state.rows.get(table) else {
                    return ResponseTemplate::new(404);
                };
                if let Some(id) = id {
                    return rows
                        .iter()
                        .find(|row| row["sys_id"] == id)
                        .map_or(ResponseTemplate::new(404), |row| {
                            ResponseTemplate::new(200).set_body_json(json!({"result":row}))
                        });
                }
                let rows: Vec<_> = rows
                    .iter()
                    .filter(|row| {
                        query.split('^').all(|part| {
                            if let Some(number) = part.strip_prefix("number=") {
                                row["number"] == number
                            } else if let Some(id) = part.strip_prefix("sys_id=") {
                                row["sys_id"] == id
                            } else if let Some((field, value)) = part.split_once('=') {
                                let field_value = row.get(field).and_then(|value| {
                                    value
                                        .as_str()
                                        .or_else(|| value.get("value").and_then(Value::as_str))
                                });
                                field_value == Some(value)
                            } else {
                                true
                            }
                        })
                    })
                    .cloned()
                    .collect();
                return ResponseTemplate::new(200).set_body_json(json!({"result":rows}));
            }
            let body: Value = request.body_json().expect("JSON write");
            assert!(
                !body.as_object().unwrap().contains_key("number"),
                "record number is not writable"
            );
            if request.method == "PATCH" {
                let row = state
                    .rows
                    .get_mut(table)
                    .and_then(|rows| {
                        rows.iter_mut()
                            .find(|row| Some(row["sys_id"].as_str().unwrap()) == id)
                    })
                    .expect("known write target");
                for (key, value) in body.as_object().unwrap() {
                    row[key] = value.clone();
                }
                row["sys_updated_on"] = json!("2026-09-01 12:01:00");
                row["sys_mod_count"] = json!("2");
                if table == "time_card" {
                    row["total"] = row["monday"].clone();
                }
                let result = row.clone();
                state.writes.push((table.into(), body));
                return ResponseTemplate::new(200).set_body_json(json!({"result":result}));
            }
            if request.method == "POST"
                && [
                    "kb_knowledge",
                    "change_request",
                    "change_task",
                    "rm_story",
                    "rm_scrum_task",
                ]
                .contains(&table)
            {
                let (sys_id, number) = match table {
                    "change_request" => ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "CHG002"),
                    "change_task" => ("cccccccccccccccccccccccccccccccc", "CTASK002"),
                    "rm_story" => ("dddddddddddddddddddddddddddddddd", "STRY002"),
                    "rm_scrum_task" => ("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "STSK002"),
                    _ => ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "KB0012345"),
                };
                let mut row = record(table, sys_id, number);
                for (key, value) in body.as_object().unwrap() {
                    row[key] = value.clone();
                }
                if table == "change_request" {
                    for index in 1..=3 {
                        let mut task = record(
                            "change_task",
                            &format!("planning-{index}"),
                            &format!("CTASK10{index}"),
                        );
                        task["change_request"] = json!(sys_id);
                        task["change_request.number"] = json!("CHG002");
                        task["change_task_type"] = json!("planning");
                        state.rows.get_mut("change_task").unwrap().push(task);
                    }
                }
                if table == "change_task" {
                    row["change_request.number"] = json!("CHG002");
                }
                state.rows.get_mut(table).unwrap().push(row.clone());
                state.writes.push((table.into(), body));
                return ResponseTemplate::new(201).set_body_json(json!({"result":row}));
            }
            ResponseTemplate::new(405)
        })
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
    for tool in [
        "change_request_apply_update",
        "change_task_apply_update",
        "incident_apply_update",
        "knowledge_apply_create_draft",
        "timecard_apply_set_hours",
        "change_request_apply_create",
        "change_task_apply_create",
    ] {
        policy.tools.get_mut(tool).unwrap().enabled = true;
    }
    let board: snow_mcp::domain::policy::BoardBinding = serde_json::from_value(json!({"name":"Example board","instance_host":instance.uri(),"story_table":"rm_story","task_table":"rm_scrum_task","column_field":"sprint","swim_lane_field":"epic","assignment_group":GROUP,"allowed_sprints":[SPRINT]})).unwrap();
    policy.boards.insert("example-board".into(), board);
    for tool in [
        "story_plan_update",
        "story_apply_update",
        "story_plan_create",
        "story_apply_create",
        "story_task_plan_create",
        "story_task_apply_create",
        "resource_plan_plan_decision",
        "resource_plan_apply_decision",
    ] {
        let mut tool_policy = snow_mcp::domain::policy::ToolPolicy {
            enabled: true,
            environments: vec!["test".into()],
            requires_confirmation: tool.contains("apply"),
            ..Default::default()
        };
        tool_policy.field_allowlist = [
            "state",
            "percent_complete",
            "short_description",
            "description",
            "acceptance_criteria",
            "assigned_to",
            "assignment_group",
            "sprint",
            "active",
            "backlog_type",
            "story",
            "planned_hours",
            "type",
            "due_date",
            "u_points_est",
            "u_story_owner",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        if tool.starts_with("story_") {
            tool_policy.story_board_id = Some("example-board".into());
        }
        policy.tools.insert(tool.into(), tool_policy);
    }
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("surface-writes"),
        snow_mcp::McpConfig {
            environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
            policy,
            ..Default::default()
        },
    ));
    let socket = fixture.tempdir.path().join("surface.sock");
    let mullet_root =
        std::env::var("MULLET_STORY_TEST_ROOT").expect("explicit Mullet checkout required");
    let node = std::env::var("MULLET_STORY_TEST_NODE").expect("explicit Node required");
    tokio::task::LocalSet::new()
        .run_until(async {
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let rpc =
                super::JsonRpcServer::new(state, snow_core::ipc::IpcEndpoint::filesystem(&socket));
            let server = tokio::task::spawn_local(rpc.serve_until(async {
                let _ = stopped.await;
                Ok(())
            }));
            let outcome = tokio::time::timeout(std::time::Duration::from_secs(60), async {
                while !socket.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                tokio::process::Command::new(node)
                    .args([
                        "--import",
                        "tsx",
                        "tests/helpers/servicenow-surface-driver.ts",
                    ])
                    .arg(&socket)
                    .current_dir(mullet_root)
                    .kill_on_drop(true)
                    .output()
                    .await
            })
            .await;
            let _ = stop.send(());
            server.await.unwrap().unwrap();
            let output = outcome.expect("E2E deadline").unwrap();
            assert!(
                output.status.success(),
                "MCP driver: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            print!("{}", String::from_utf8_lossy(&output.stdout));
        })
        .await;
    let state = provider.lock().unwrap();
    assert_eq!(
        state.writes.len(),
        12,
        "invalid confirmation and exact-token replay must never write"
    );
    assert_eq!(
        state.rows["change_request"][0]["short_description"],
        "MCP changed the request"
    );
    assert_eq!(
        state.rows["change_task"][0]["short_description"],
        "MCP changed the task"
    );
    assert_eq!(state.rows["incident"][0]["assigned_to"], "");
    assert_eq!(state.rows["rm_story"][0]["percent_complete"], 25);
    assert_eq!(
        state.rows["rm_story"][1]["short_description"],
        "MCP created story"
    );
    assert_eq!(
        state.rows["rm_scrum_task"][0]["short_description"],
        "MCP created story task"
    );
    assert_eq!(
        state.rows["rm_scrum_task"][0]["story"],
        "dddddddddddddddddddddddddddddddd"
    );
    assert_eq!(state.rows["resource_plan"][0]["state"], "11");
    assert_eq!(state.rows["time_card"][0]["monday"], "2");
    assert_eq!(state.rows["kb_knowledge"][0]["workflow_state"], "draft");
    assert_eq!(
        state.rows["change_request"][1]["short_description"],
        "MCP created change"
    );
    let implementation = state.rows["change_task"]
        .iter()
        .find(|row| row["number"] == "CTASK002")
        .unwrap();
    assert_eq!(implementation["change_task_type"], "implementation");
    assert_eq!(
        implementation["change_request"],
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    );
    assert_eq!(
        state.rows["kb_knowledge"][0]["text"],
        "<p>Independent article body.</p>"
    );
}
